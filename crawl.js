#!/usr/bin/env node
/**
 * ============================================================================
 *  CRAWLER PNCP → JSON  (para rodar em GitHub Actions)
 * ============================================================================
 *  Varre o PNCP por período e grava partições mensais enxutas em data/AAAA-MM.json.
 *  Idempotente: reprocessar um período apenas mescla (dedupe por chave), não duplica.
 *
 *  MODOS:
 *    node crawl.js --backfill 12      # últimos 12 meses (pula meses já prontos)
 *    node crawl.js --month 2026-03    # um mês específico
 *    node crawl.js --range 20260101 20260131
 *    node crawl.js --yesterday        # só o dia anterior (delta diário)
 *    node crawl.js --day 20260710      # um dia específico
 *  Flags:
 *    FORCE=1        reprocessa mesmo meses já existentes
 *    RESULTS=0      desativa a busca por homologados (padrão: ativo, busca homologados)
 *    UF=RJ          restringe a uma UF (padrão: vazio = Brasil inteiro)
 *    USE_ATUALIZACAO=1 usa o endpoint de atualização no range/day
 * ============================================================================
 */

const fs = require('fs');
const path = require('path');

const CFG = {
  CONSULTA: 'https://pncp.gov.br/api/consulta',
  PNCP: 'https://pncp.gov.br/api/pncp',
  UF: process.env.UF || '',                                      // Vazio = todas as UFs (Brasil)
  MODALIDADES: (process.env.MODALIDADES || '6,8').split(',').map(Number), // 6=Pregão 8=Dispensa
  TAMANHO_PAGINA: 50,                                            // máximo aceito
  CONCORRENCIA: Number(process.env.CONCORRENCIA || 3),           // Reduzido para 3 para evitar rate limiting (429)
  RESULTS: process.env.RESULTS !== '0',                          // Ativo por padrão
  FORCE: process.env.FORCE === '1',
  USE_ATUALIZACAO: process.env.USE_ATUALIZACAO === '1',
  OUT: path.join(process.cwd(), 'data')
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ---- fetch com retry/backoff (PNCP cai às vezes) ---- */
async function getJson(url, retries = 4) {
  for (let a = 0; a <= retries; a++) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), 12000); // 12 segundos de timeout
    try {
      const r = await fetch(url, { 
        headers: { accept: '*/*' },
        signal: controller.signal
      });
      clearTimeout(id);
      if (r.status === 200) return await r.json();
      if (r.status === 204) return null;
      if (r.status === 400) { const e = new Error('HTTP400'); e.code = 400; e.body = await r.text(); throw e; }
      if (r.status === 429) {
        const tempoEspera = 3000 * Math.pow(2, a);
        console.warn(`  [429] Rate limit atingido no PNCP. Aguardando ${tempoEspera / 1000}s antes de tentar novamente...`);
        await sleep(tempoEspera);
        throw new Error('HTTP429');
      }
      if (r.status >= 500) throw new Error('HTTP' + r.status);
      return null;                              // outros 4xx: ignora
    } catch (err) {
      clearTimeout(id);
      if (err.code === 400) throw err;
      if (a === retries) { console.error('  ! falha', url.slice(-60), err.message); return null; }
      await sleep(1500 * Math.pow(2, a));       // Recuo padrão aumentado para 1.5s
    }
  }
}

/* ---- pool de concorrência simples (sem dependências) ---- */
async function pool(items, size, worker) {
  const out = new Array(items.length);
  let i = 0;
  await Promise.all(Array.from({ length: Math.min(size, items.length) }, async () => {
    while (i < items.length) { const idx = i++; out[idx] = await worker(items[idx], idx); }
  }));
  return out;
}

/* ---- pool de concorrência simples (sem dependências) ---- */
const fmt = d => d.toISOString().slice(0, 10).replace(/-/g, '');
const parse = s => new Date(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8));
function eachDay(iniYmd, fimYmd) {
  const out = []; const a = parse(iniYmd), b = parse(fimYmd);
  for (let d = new Date(a); d <= b; d.setDate(d.getDate() + 1)) out.push(fmt(new Date(d)));
  return out;
}

/* ---- URLs ---- */
const urlContrat = (dia, mod, pag, useAtualizacao = false) =>
  `${CFG.CONSULTA}/v1/contratacoes/${useAtualizacao ? 'atualizacao' : 'publicacao'}?dataInicial=${dia}&dataFinal=${dia}` +
  `&codigoModalidadeContratacao=${mod}${CFG.UF ? '&uf=' + CFG.UF : ''}` +
  `&pagina=${pag}&tamanhoPagina=${CFG.TAMANHO_PAGINA}`;
const urlItens = c => `${CFG.PNCP}/v1/orgaos/${c.cnpj}/compras/${c.ano}/${c.seq}/itens`;
const urlResult = (c, n) => `${urlItens(c)}/${n}/resultados`;

/* ---- listar contratações de UM dia (todas as páginas, todas as modalidades) ---- */
async function listarDia(dia, useAtualizacao = false) {
  const compras = [];
  for (const mod of CFG.MODALIDADES) {
    const p1 = await getJson(urlContrat(dia, mod, 1, useAtualizacao));
    if (!p1 || !p1.data) continue;
    const total = p1.totalPaginas || 1;
    const push = arr => arr.forEach(c => {
      const cnpj = c.orgaoEntidade && c.orgaoEntidade.cnpj;
      if (cnpj && c.anoCompra && c.sequencialCompra) {
        compras.push({
          cnpj, 
          ano: c.anoCompra, 
          seq: c.sequencialCompra,
          orgao: c.orgaoEntidade.razaoSocial,
          uf: (c.unidadeOrgao && c.unidadeOrgao.ufSigla) || CFG.UF,
          data: (c.dataPublicacaoPncp || '').slice(0, 10),
          objeto: c.objetoCompra || '', 
          mod
        });
      }
    });
    push(p1.data);
    if (total > 1) {
      const pags = [];
      for (let p = 2; p <= total; p++) pags.push(p);
      const resps = await pool(pags, CFG.CONCORRENCIA, p => getJson(urlContrat(dia, mod, p, useAtualizacao)));
      resps.forEach(r => { if (r && r.data) push(r.data); });
    }
  }
  return compras;
}

/* ---- baixar itens de uma lista de contratações → registros enxutos ---- */
async function itensDe(compras) {
  const registros = [];
  const listas = await pool(compras, CFG.CONCORRENCIA, c => getJson(urlItens(c)));
  const paraResultado = [];
  
  listas.forEach((itens, i) => {
    if (!Array.isArray(itens)) return;
    const c = compras[i];
    itens.forEach(it => {
      const vu = Number(it.valorUnitarioEstimado);
      if (!(vu > 0)) return;
      const reg = {
        k: `${c.cnpj}-${c.ano}-${c.seq}-${it.numeroItem}`,
        d: it.descricao || '',
        vu,
        un: it.unidadeMedida || '',
        dt: c.data,
        or: c.orgao,
        uf: c.uf,
        ts: /s/i.test(String(it.materialOuServico)) && !/m/i.test(String(it.materialOuServico)) ? 'S' : 'M',
        cod: it.catalogoCodigoItem || it.ncmNbsCodigo || '',
        mod: c.mod,
        h: 0                                                     // 0=estimado, 1=homologado
      };
      
      if (CFG.RESULTS) {
        paraResultado.push({ c, n: it.numeroItem, reg });
      } else {
        registros.push(reg); // se não busca resultados, guarda o estimado
      }
    });
  });
  
  if (CFG.RESULTS && paraResultado.length) {
    await pool(paraResultado, CFG.CONCORRENCIA, async x => {
      const res = await getJson(urlResult(x.c, x.n));
      if (Array.isArray(res) && res.length) {
        const v = Number(res[0].valorUnitarioHomologado);
        if (v > 0) { 
          x.reg.vu = v; 
          x.reg.h = 1; 
          registros.push(x.reg); // SÓ SALVA SE FOR HOMOLOGADO!
        }
      }
    });
  }
  return registros;
}

/* ---- crawl de um intervalo → registros ---- */
async function crawlRange(iniYmd, fimYmd, useAtualizacao = false) {
  const dias = eachDay(iniYmd, fimYmd);
  let todos = [];
  for (const dia of dias) {
    const compras = await listarDia(dia, useAtualizacao);
    const regs = await itensDe(compras);
    todos = todos.concat(regs);
    console.log(`  ${dia}: ${compras.length} contratações, ${regs.length} itens homologados (acum ${todos.length})`);
  }
  return todos;
}

/* ---- gravação com merge/dedupe por partição mensal ---- */
function mesDe(dtISO) { return (dtISO || '').slice(0, 7) || 'sem-data'; }
function mergeMes(mes, novos) {
  if (!fs.existsSync(CFG.OUT)) fs.mkdirSync(CFG.OUT, { recursive: true });
  const arq = path.join(CFG.OUT, `${mes}.json`);
  const mapa = new Map();
  if (fs.existsSync(arq)) {
    try {
      JSON.parse(fs.readFileSync(arq, 'utf8')).forEach(r => mapa.set(r.k, r));
    } catch(e) {
      console.error(`Erro ao ler arquivo existente ${arq}:`, e.message);
    }
  }
  novos.forEach(r => mapa.set(r.k, r));
  const arr = [...mapa.values()];
  fs.writeFileSync(arq, JSON.stringify(arr));
  return arr.length;
}
function gravar(registros) {
  const porMes = {};
  registros.forEach(r => { (porMes[mesDe(r.dt)] = porMes[mesDe(r.dt)] || []).push(r); });
  const resumo = {};
  for (const mes of Object.keys(porMes)) resumo[mes] = mergeMes(mes, porMes[mes]);
  atualizarManifest();
  return resumo;
}
function atualizarManifest() {
  if (!fs.existsSync(CFG.OUT)) return;
  const meses = fs.readdirSync(CFG.OUT).filter(f => /^\d{4}-\d{2}\.json$/.test(f))
    .map(f => {
      const arr = JSON.parse(fs.readFileSync(path.join(CFG.OUT, f), 'utf8'));
      return { mes: f.replace('.json', ''), itens: arr.length };
    }).sort((a, b) => b.mes.localeCompare(a.mes));
  fs.writeFileSync(path.join(CFG.OUT, 'manifest.json'),
    JSON.stringify({ atualizadoEm: new Date().toISOString(), uf: CFG.UF, meses }, null, 2));
  console.log('manifest:', meses.map(m => `${m.mes}:${m.itens}`).join('  '));
}

/* ---- crawl de um mês (com skip se já existe) ---- */
async function crawlMes(ym) {
  const arq = path.join(CFG.OUT, `${ym}.json`);
  if (!CFG.FORCE && fs.existsSync(arq)) { console.log(`= ${ym} já existe, pulando (FORCE=1 para refazer)`); return; }
  const [y, m] = ym.split('-').map(Number);
  const ini = fmt(new Date(y, m - 1, 1));
  const fim = fmt(new Date(y, m, 0));         // último dia do mês
  console.log(`\n== Mês ${ym} (${ini}..${fim}) ==`);
  const regs = await crawlRange(ini, fim, CFG.USE_ATUALIZACAO);
  console.log('  gravado:', gravar(regs));
}

/* ---- CLI ---- */
async function main() {
  const [mode, a, b] = process.argv.slice(2);
  const t0 = Date.now();
  console.log(`PNCP crawler | UF=${CFG.UF || 'todas (Brasil)'} | modalidades=${CFG.MODALIDADES} | homologados_apenas=${CFG.RESULTS} | use_atualizacao=${CFG.USE_ATUALIZACAO}`);

  if (mode === '--backfill') {
    const n = Number(a || 12);
    const hoje = new Date();
    for (let i = 0; i < n; i++) {
      const d = new Date(hoje.getFullYear(), hoje.getMonth() - i, 1);
      const mesStr = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      await crawlMes(mesStr);
    }
  } else if (mode === '--month') {
    await crawlMes(a);
  } else if (mode === '--range') {
    console.log(gravar(await crawlRange(a, b, CFG.USE_ATUALIZACAO)));
  } else if (mode === '--yesterday') {
    const y = new Date(Date.now() - 86400000);
    // Para o delta diário, usamos o endpoint de ATUALIZAÇÃO por padrão
    console.log(gravar(await crawlRange(fmt(y), fmt(y), true)));
  } else if (mode === '--day') {
    console.log(gravar(await crawlRange(a, a, CFG.USE_ATUALIZACAO)));
  } else {
    console.log('Uso: --backfill N | --month AAAA-MM | --range INI FIM | --yesterday | --day AAAAMMDD');
    process.exit(1);
  }
  console.log(`\nConcluído em ${((Date.now() - t0) / 1000) | 0}s`);
}
main().catch(e => { console.error(e); process.exit(1); });
