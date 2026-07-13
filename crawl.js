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
 *    RESULTS=1      também busca o valor HOMOLOGADO (mais lento; padrão: estimado)
 *
 *  Campos confirmados no retorno real do PNCP (log de 13/07/2026).
 * ============================================================================
 */

const fs = require('fs');
const path = require('path');

const CFG = {
  CONSULTA: 'https://pncp.gov.br/api/consulta',
  PNCP: 'https://pncp.gov.br/api/pncp',
  UF: process.env.UF || 'RJ',                 // '' = todas
  MODALIDADES: (process.env.MODALIDADES || '6,8').split(',').map(Number), // 6=Pregão 8=Dispensa
  TAMANHO_PAGINA: 50,                          // máximo aceito
  CONCORRENCIA: Number(process.env.CONCORRENCIA || 12),
  RESULTS: process.env.RESULTS === '1',
  FORCE: process.env.FORCE === '1',
  OUT: path.join(process.cwd(), 'data')
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ---- fetch com retry/backoff (PNCP cai às vezes) ---- */
async function getJson(url, retries = 4) {
  for (let a = 0; a <= retries; a++) {
    try {
      const r = await fetch(url, { headers: { accept: '*/*' } });
      if (r.status === 200) return await r.json();
      if (r.status === 204) return null;
      if (r.status === 400) { const e = new Error('HTTP400'); e.code = 400; e.body = await r.text(); throw e; }
      if (r.status >= 500 || r.status === 429) throw new Error('HTTP' + r.status);
      return null;                              // outros 4xx: ignora
    } catch (err) {
      if (err.code === 400) throw err;
      if (a === retries) { console.error('  ! falha', url.slice(-60), err.message); return null; }
      await sleep(600 * Math.pow(2, a));
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

/* ---- datas yyyymmdd ---- */
const fmt = d => d.toISOString().slice(0, 10).replace(/-/g, '');
const parse = s => new Date(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8));
function eachDay(iniYmd, fimYmd) {
  const out = []; const a = parse(iniYmd), b = parse(fimYmd);
  for (let d = new Date(a); d <= b; d.setDate(d.getDate() + 1)) out.push(fmt(new Date(d)));
  return out;
}

/* ---- URLs ---- */
const urlContrat = (dia, mod, pag) =>
  `${CFG.CONSULTA}/v1/contratacoes/publicacao?dataInicial=${dia}&dataFinal=${dia}` +
  `&codigoModalidadeContratacao=${mod}${CFG.UF ? '&uf=' + CFG.UF : ''}` +
  `&pagina=${pag}&tamanhoPagina=${CFG.TAMANHO_PAGINA}`;
const urlItens = c => `${CFG.PNCP}/v1/orgaos/${c.cnpj}/compras/${c.ano}/${c.seq}/itens`;
const urlResult = (c, n) => `${urlItens(c)}/${n}/resultados`;

/* ---- listar contratações de UM dia (todas as páginas, todas as modalidades) ---- */
async function listarDia(dia) {
  const compras = [];
  for (const mod of CFG.MODALIDADES) {
    const p1 = await getJson(urlContrat(dia, mod, 1));
    if (!p1 || !p1.data) continue;
    const total = p1.totalPaginas || 1;
    const push = arr => arr.forEach(c => {
      const cnpj = c.orgaoEntidade && c.orgaoEntidade.cnpj;
      if (cnpj && c.anoCompra && c.sequencialCompra)
        compras.push({
          cnpj, ano: c.anoCompra, seq: c.sequencialCompra,
          orgao: c.orgaoEntidade.razaoSocial,
          uf: (c.unidadeOrgao && c.unidadeOrgao.ufSigla) || CFG.UF,
          data: (c.dataPublicacaoPncp || '').slice(0, 10),
          objeto: c.objetoCompra || '', mod
        });
    });
    push(p1.data);
    if (total > 1) {
      const pags = [];
      for (let p = 2; p <= total; p++) pags.push(p);
      const resps = await pool(pags, CFG.CONCORRENCIA, p => getJson(urlContrat(dia, mod, p)));
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
        h: 0                                    // 0=estimado, 1=homologado
      };
      registros.push(reg);
      if (CFG.RESULTS) paraResultado.push({ c, n: it.numeroItem, reg });
    });
  });
  if (CFG.RESULTS && paraResultado.length) {
    await pool(paraResultado, CFG.CONCORRENCIA, async x => {
      const res = await getJson(urlResult(x.c, x.n));
      if (Array.isArray(res) && res.length) {
        const v = Number(res[0].valorUnitarioHomologado);
        if (v > 0) { x.reg.vu = v; x.reg.h = 1; }
      }
    });
  }
  return registros;
}

/* ---- crawl de um intervalo → registros ---- */
async function crawlRange(iniYmd, fimYmd) {
  const dias = eachDay(iniYmd, fimYmd);
  let todos = [];
  for (const dia of dias) {
    const compras = await listarDia(dia);
    const regs = await itensDe(compras);
    todos = todos.concat(regs);
    console.log(`  ${dia}: ${compras.length} contratações, ${regs.length} itens (acum ${todos.length})`);
  }
  return todos;
}

/* ---- gravação com merge/dedupe por partição mensal ---- */
function mesDe(dtISO) { return (dtISO || '').slice(0, 7) || 'sem-data'; }
function mergeMes(mes, novos) {
  if (!fs.existsSync(CFG.OUT)) fs.mkdirSync(CFG.OUT, { recursive: true });
  const arq = path.join(CFG.OUT, `${mes}.json`);
  const mapa = new Map();
  if (fs.existsSync(arq)) JSON.parse(fs.readFileSync(arq, 'utf8')).forEach(r => mapa.set(r.k, r));
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
  const regs = await crawlRange(ini, fim);
  console.log('  gravado:', gravar(regs));
}

/* ---- CLI ---- */
async function main() {
  const [mode, a, b] = process.argv.slice(2);
  const t0 = Date.now();
  console.log(`PNCP crawler | UF=${CFG.UF || 'todas'} | modalidades=${CFG.MODALIDADES} | homologado=${CFG.RESULTS}`);

  if (mode === '--backfill') {
    const n = Number(a || 12);
    const hoje = new Date();
    for (let i = 0; i < n; i++) {
      const d = new Date(hoje.getFullYear(), hoje.getMonth() - i, 1);
      await crawlMes(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`);
    }
  } else if (mode === '--month') {
    await crawlMes(a);
  } else if (mode === '--range') {
    console.log(gravar(await crawlRange(a, b)));
  } else if (mode === '--yesterday') {
    const y = new Date(Date.now() - 86400000);
    console.log(gravar(await crawlRange(fmt(y), fmt(y))));
  } else if (mode === '--day') {
    console.log(gravar(await crawlRange(a, a)));
  } else {
    console.log('Uso: --backfill N | --month AAAA-MM | --range INI FIM | --yesterday | --day AAAAMMDD');
    process.exit(1);
  }
  console.log(`\nConcluído em ${((Date.now() - t0) / 1000 | 0)}s`);
}
main().catch(e => { console.error(e); process.exit(1); });
