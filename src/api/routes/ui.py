# -*- coding: utf-8 -*-
"""Web UI for browsing and filtering leads."""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

_RUB = "\u20bd"  # ₽


@router.get("/", response_class=HTMLResponse)
async def leads_dashboard() -> str:
    """Serve the leads dashboard as a single-page HTML application."""
    return _build_html()


def _build_html() -> str:
    """Build the full HTML page for the leads dashboard."""
    return _CSS + _BODY + _js() + "\n</body>\n</html>"


_CSS = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Glukhov Sales Engine</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  background:#f5f7fa;color:#1a1a2e;font-size:14px}
.header{background:#1a1a2e;color:#fff;padding:16px 24px;display:flex;
  align-items:center;justify-content:space-between}
.header h1{font-size:18px;font-weight:600}
.header .stats{font-size:13px;opacity:.8}
.container{max-width:1440px;margin:0 auto;padding:16px}
.filters{background:#fff;border-radius:8px;padding:16px;margin-bottom:16px;
  box-shadow:0 1px 3px rgba(0,0,0,.08);display:flex;flex-wrap:wrap;gap:10px;
  align-items:flex-end}
.fg{display:flex;flex-direction:column;gap:3px}
.fg label{font-size:11px;font-weight:600;color:#666;text-transform:uppercase;
  letter-spacing:.5px}
.fg select,.fg input{padding:6px 10px;border:1px solid #ddd;border-radius:4px;
  font-size:13px;min-width:130px;background:#fff}
.btn{padding:6px 16px;border:none;border-radius:4px;cursor:pointer;
  font-size:13px;font-weight:500;transition:background .15s}
.btn-p{background:#4361ee;color:#fff}.btn-p:hover{background:#3a56d4}
.btn-s{background:#e9ecef;color:#333}.btn-s:hover{background:#dee2e6}
.tw{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.08);
  overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{background:#f8f9fa;padding:10px 12px;text-align:left;font-size:11px;
  font-weight:600;color:#666;text-transform:uppercase;letter-spacing:.5px;
  border-bottom:2px solid #e9ecef;white-space:nowrap}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:#4361ee}
th .arrow{font-size:10px;margin-left:3px}
td{padding:10px 12px;border-bottom:1px solid #f0f0f0;font-size:13px;
  vertical-align:top;max-width:300px}
tr:hover{background:#f8f9ff}
.b{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;
  font-weight:500;margin:1px 2px}
.b-src{background:#e8f4fd;color:#1976d2}
.b-tag{background:#f3e5f5;color:#7b1fa2}
.b-kw{background:#e0f2f1;color:#00695c}
.b-cat{background:#fce4ec;color:#ad1457}
.b-ok{background:#fff8e1;color:#f57f17}
.bud{font-weight:600;color:#2e7d32;white-space:nowrap}
.bud-na{color:#999;font-style:italic;font-size:12px;white-space:nowrap}
.ul{color:#4361ee;text-decoration:none;word-break:break-all}
.ul:hover{text-decoration:underline}
.dc{max-width:450px;position:relative}
.dc-text{overflow:hidden;text-overflow:ellipsis;display:-webkit-box;
  -webkit-line-clamp:2;-webkit-box-orient:vertical}
.dc-text.expanded{-webkit-line-clamp:unset;display:block;white-space:pre-wrap}
.dc-extra{margin-top:6px;padding-top:6px;border-top:1px dashed #e0e0e0;font-size:12px;color:#555}
.dc-extra span{display:inline-block;margin:2px 4px 2px 0}
.dc-toggle{color:#4361ee;cursor:pointer;font-size:11px;margin-top:3px;
  display:inline-block;user-select:none}
.dc-toggle:hover{text-decoration:underline}
.pg{display:flex;align-items:center;justify-content:center;gap:8px;padding:16px}
.pg button{padding:6px 12px;border:1px solid #ddd;border-radius:4px;
  background:#fff;cursor:pointer;font-size:13px}
.pg button:hover:not(:disabled){background:#f0f0f0}
.pg button:disabled{opacity:.4;cursor:default}
.pg .pi{font-size:13px;color:#666}
.em{text-align:center;padding:48px;color:#999;font-size:15px}
.ss{padding:3px 6px;border:1px solid #ddd;border-radius:4px;font-size:12px;
  cursor:pointer;background:#fff}
.ld{text-align:center;padding:48px;color:#999}
.dt{white-space:nowrap;font-size:12px;color:#666}
</style>
</head>
"""

_BODY = """<body>
<div class="header">
  <h1>Glukhov Sales Engine</h1>
  <div class="stats" id="stats">...</div>
</div>
<div class="container">
  <div class="filters" id="filters">
    <div class="fg"><label>Источник</label>
      <select id="fs"><option value="">Все</option></select></div>
    <div class="fg"><label>Статус</label>
      <select id="ft"><option value="">Все</option></select></div>
    <div class="fg"><label>Категория</label>
      <select id="fc"><option value="">Все</option></select></div>
    <div class="fg"><label>Ключевое слово</label>
      <input type="text" id="fk" placeholder="python"></div>
    <div class="fg"><label>Тег</label>
      <select id="fg"><option value="">Все</option>
        <option value="urgent">urgent</option>
        <option value="normal">normal</option></select></div>
    <div class="fg"><label>ОКПД2</label>
      <input type="text" id="fo" placeholder="62.01"></div>
    <div class="fg"><label>Дата от</label>
      <input type="date" id="fd1"></div>
    <div class="fg"><label>Дата до</label>
      <input type="date" id="fd2"></div>
    <div class="fg" style="justify-content:flex-end">
      <div style="display:flex;gap:6px">
        <button class="btn btn-p" onclick="af()">Найти</button>
        <button class="btn btn-s" onclick="rf()">Сброс</button>
      </div>
    </div>
  </div>
  <div class="tw"><table><thead><tr>
    <th class="sortable" onclick="ss('source')">Источник<span class="arrow" id="arr-source"></span></th>
    <th>Статус</th>
    <th class="sortable" onclick="ss('title')">Заголовок<span class="arrow" id="arr-title"></span></th>
    <th>Описание</th>
    <th class="sortable" onclick="ss('budget')">Бюджет<span class="arrow" id="arr-budget"></span></th>
    <th>Метки</th>
    <th class="sortable" onclick="ss('discovered_at')">Найден<span class="arrow" id="arr-discovered_at"></span></th>
  </tr></thead><tbody id="tb">
    <tr><td colspan="7" class="ld">Загрузка...</td></tr>
  </tbody></table></div>
  <div class="pg" id="pg"></div>
</div>
"""


def _js() -> str:
    """Return the <script> block with the ruble sign injected."""
    rub = _RUB
    return f"""<script>
const A='/api/v1',NC=7;let cp=1;const pp=30;
let sortBy='created_at',sortDir='desc';
const SL={{'new':'\\u041D\\u043E\\u0432\\u0430\\u044F','viewed':'\\u041F\\u0440\\u043E\\u0441\\u043C\\u043E\\u0442\\u0440\\u0435\\u043D\\u043E','in_progress':'\\u0412 \\u0440\\u0430\\u0431\\u043E\\u0442\\u0435','rejected':'\\u041E\\u0442\\u043A\\u043B\\u043E\\u043D\\u0435\\u043D\\u043E'}};
async function lo(){{try{{const r=await fetch(A+'/leads-filter-options');const d=await r.json();
fsl('fs',d.sources||[]);fsl('ft',d.statuses||[]);fsl('fc',d.categories||[]);}}catch(e){{console.error(e)}}}}
function fsl(id,vs){{const s=document.getElementById(id);const c=s.value;
while(s.options.length>1)s.remove(1);vs.forEach(v=>{{const o=document.createElement('option');
o.value=v;o.textContent=v;s.appendChild(o)}});s.value=c}}
function gf(){{const p=new URLSearchParams();
const v=id=>document.getElementById(id).value;
if(v('fs'))p.set('source',v('fs'));if(v('ft'))p.set('status',v('ft'));
if(v('fc'))p.set('category',v('fc'));if(v('fk').trim())p.set('keyword',v('fk').trim());
if(v('fg'))p.set('tags',v('fg'));if(v('fo').trim())p.set('okpd2',v('fo').trim());
if(v('fd1'))p.set('date_from',v('fd1')+'T00:00:00Z');
if(v('fd2'))p.set('date_to',v('fd2')+'T23:59:59Z');
p.set('sort_by',sortBy);p.set('sort_dir',sortDir);return p}}
function ss(col){{if(sortBy===col){{sortDir=sortDir==='desc'?'asc':'desc'}}else{{sortBy=col;sortDir='desc'}};ua();ll(1)}}
function ua(){{document.querySelectorAll('th .arrow').forEach(a=>a.textContent='');
const el=document.getElementById('arr-'+sortBy);
if(el)el.textContent=sortDir==='asc'?' \\u25B2':' \\u25BC'}}
async function ll(pg){{cp=pg||1;const p=gf();p.set('page',cp);p.set('per_page',pp);
const b=document.getElementById('tb');
b.innerHTML='<tr><td colspan="'+NC+'" class="ld">\\u0417\\u0430\\u0433\\u0440\\u0443\\u0437\\u043A\\u0430...</td></tr>';
try{{const r=await fetch(A+'/leads?'+p);const d=await r.json();rl(d);rp(d);
document.getElementById('stats').textContent=
'\\u0412\\u0441\\u0435\\u0433\\u043E: '+d.total+' | \\u0421\\u0442\\u0440. '+d.page+' / '+(Math.ceil(d.total/d.per_page)||1);
}}catch(er){{b.innerHTML='<tr><td colspan="'+NC+'" class="em">\\u041E\\u0448\\u0438\\u0431\\u043A\\u0430</td></tr>';console.error(er)}}}}
function rl(d){{const b=document.getElementById('tb');
if(!d.items||!d.items.length){{b.innerHTML='<tr><td colspan="'+NC+'" class="em">\\u041D\\u0435\\u0442 \\u043B\\u0438\\u0434\\u043E\\u0432</td></tr>';return}}
b.innerHTML=d.items.map((l,i)=>{{
const desc=l.description||'';
const extra=xtra(l);
const hasExtra=desc.length>120||extra;
return '<tr>'+
'<td><span class="b b-src">'+e(l.source)+'</span></td>'+
'<td><select class="ss" onchange="us(\\''+l.id+'\\',this.value)">'+
Object.keys(SL).map(s=>
'<option value="'+s+'"'+(l.status===s?' selected':'')+'>'+(SL[s])+'</option>').join('')+
'</select></td>'+
'<td>'+(l.url?'<a class="ul" href="'+e(l.url)+'" target="_blank">'+e(l.title)+'</a>':e(l.title))+'</td>'+
'<td><div class="dc">'+
'<div class="dc-text" id="desc-'+i+'">'+e(desc)+'</div>'+
(extra?'<div class="dc-extra" id="extra-'+i+'" style="display:none">'+extra+'</div>':'')+
(hasExtra?'<span class="dc-toggle" onclick="td('+i+','+!!extra+')">\\u0420\\u0430\\u0437\\u0432\\u0435\\u0440\\u043D\\u0443\\u0442\\u044C</span>':'')+
'</div></td>'+
'<td>'+budgetCell(l)+'</td>'+
'<td>'+labelsCell(l)+'</td>'+
'<td class="dt">'+fd(l.discovered_at)+'</td>'+
'</tr>'}}).join('')}}
function budgetCell(l){{
if(l.budget)return '<span class="bud">'+Number(l.budget).toLocaleString("ru-RU")+' {rub}</span>';
return '<span class="bud-na">\\u041F\\u043E \\u0434\\u043E\\u0433\\u043E\\u0432\\u043E\\u0440\\u0451\\u043D\\u043D\\u043E\\u0441\\u0442\\u0438</span>'}}
function labelsCell(l){{
var parts=[];
if(l.category)parts.push('<span class="b b-cat">'+e(l.category)+'</span>');
(l.matched_keywords||[]).forEach(k=>parts.push('<span class="b b-kw">'+e(k)+'</span>'));
(l.tags||[]).forEach(t=>parts.push('<span class="b b-tag">'+e(t)+'</span>'));
return parts.length?parts.join(' '):'\\u2014'}}
function xtra(l){{
var parts=[];
if(l.max_contract_price)parts.push('\\u041C\\u0430\\u043A\\u0441. \\u0446\\u0435\\u043D\\u0430: <b>'+Number(l.max_contract_price).toLocaleString("ru-RU")+' {rub}</b>');
if(l.submission_deadline)parts.push('\\u0414\\u0435\\u0434\\u043B\\u0430\\u0439\\u043D: <b>'+fd(l.submission_deadline)+'</b>');
if(l.okpd2_codes&&l.okpd2_codes.length)parts.push('\\u041E\\u041A\\u041F\\u04142: '+(l.okpd2_codes.map(c=>'<span class="b b-ok">'+e(c)+'</span>').join(' ')));
return parts.length?parts.join(' &middot; '):''}}
function td(i,hasX){{const el=document.getElementById('desc-'+i);if(!el)return;
const isExp=el.classList.toggle('expanded');
if(hasX){{const x=document.getElementById('extra-'+i);if(x)x.style.display=isExp?'block':'none'}}
const tog=el.parentElement.querySelector('.dc-toggle');
if(tog)tog.textContent=isExp?'\\u0421\\u0432\\u0435\\u0440\\u043D\\u0443\\u0442\\u044C':'\\u0420\\u0430\\u0437\\u0432\\u0435\\u0440\\u043D\\u0443\\u0442\\u044C'}}
function rp(d){{const tp=Math.ceil(d.total/d.per_page)||1;const g=document.getElementById('pg');
g.innerHTML='<button onclick="ll(1)"'+(d.page<=1?' disabled':'')+'>\\u27EA</button>'+
'<button onclick="ll('+(d.page-1)+')"'+(d.page<=1?' disabled':'')+'>\\u2190</button>'+
'<span class="pi">'+d.page+' / '+tp+'</span>'+
'<button onclick="ll('+(d.page+1)+')"'+(d.page>=tp?' disabled':'')+'>\\u2192</button>'+
'<button onclick="ll('+tp+')"'+(d.page>=tp?' disabled':'')+'>\\u27EB</button>'}}
async function us(id,st){{try{{await fetch(A+'/leads/'+id,{{method:'PATCH',
headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{status:st}})}})}}catch(er){{console.error(er)}}}}
function af(){{ll(1)}}
function rf(){{['fs','ft','fc','fk','fg','fo','fd1','fd2'].forEach(id=>document.getElementById(id).value='');
sortBy='created_at';sortDir='desc';ua();ll(1)}}
function e(s){{if(!s)return'';const d=document.createElement('div');d.textContent=s;return d.innerHTML}}
function fd(iso){{if(!iso)return'\\u2014';const d=new Date(iso);
return d.toLocaleDateString('ru-RU',{{day:'2-digit',month:'2-digit',year:'numeric'}})
+' '+d.toLocaleTimeString('ru-RU',{{hour:'2-digit',minute:'2-digit'}})}}
document.getElementById('filters').addEventListener('keydown',ev=>{{if(ev.key==='Enter')af()}});
ua();lo();ll(1);
</script>"""
