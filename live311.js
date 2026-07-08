;(function(g){
'use strict';

const API='https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/311_Customer_Service_Requests_current/FeatureServer/0/query';
const GEOCODE='https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates';
const SUGGEST='https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/suggest';
const PORTAL='https://balt311.baltimorecity.gov/citizen/s/';
const OUT='ServiceRequestNum,SRType,SRStatus,CreatedDate,DueDate,CloseDate,Agency,Outcome,Address,Neighborhood,Latitude,Longitude';
const FOLLOW_KEY='balti_follows_v1';
const CACHE_TTL=120000, MIN_ZOOM=15, LIMIT=200, REPORT_ZOOM=18;
const COLORS={open:'var(--warn,#a66a00)',closed:'var(--success,#16815f)',caveat:'var(--t3,#7b7f87)'};
const CATEGORIES=[
  {id:'dirty',label:'Dirty street / alley',types:['SW-Dirty Alley','SW-Dirty Alley Proactive','SW-Dirty Street','SW-Dirty Street Proactive']},
  {id:'rats',label:'Rats',types:['SW-Rat Rubout','SW-Rat Rubout Alley Concern (SW creation only)','SW-Rat Rubout Follow-up','SW-Rat Rubout Proactive']},
  {id:'potholes',label:'Potholes',types:['TRM-Pickup Pothole','TRM-Potholes']},
  {id:'dumping',label:'Illegal dumping',types:['HCD-Illegal Dumping']},
  {id:'lights',label:'Streetlights',types:['BGE-StLight(s) Out','BGE-StLight(s) Out Rear','BGE-StLighting Cable Faults','RP-Street Lighting Repairs','TRM-StLight Damaged/Knocked Down/Rusted','TRM-StLight Pole Access Cover/Plate Missing','TRM-StLight Pole Missing','TRM-StLighting Inadequate/Too Bright','TRM-Street Light Out']},
  {id:'trees',label:'Trees / limbs',types:['FOR-Broken Branch in Tree','FOR-Down Tree','FOR-Fallen Limb','FOR-Tree Maintenance']},
  {id:'sanprop',label:'Sanitation property',types:['HCD-Sanitation Property']},
  {id:'vehicles',label:'Abandoned vehicle',types:['HCD-Abandoned Vehicle','TRS-48 Hour Parking/Abandoned Vehicle','TRS-48 Hour Parking/Abandoned Vehicle (DOT SAFETY USE ONLY)','TRS-48 Hour Parking/Abandoned Vehicle (DOT USE ONLY)','TTR-Abandoned Vehicle Turn-in Program']},
  {id:'water',label:'Water leak',types:['WW-Hydrant Leaking','WW-Water Leak (Exterior)','WW-Water Meter Leak']},
  {id:'bulk',label:'Bulk pickup',types:['SW-Bulk Scheduled-Saturday','SW-Bulk Scheduled-Weekday','SW-Bulk Special','SW-RP Bulk Pickup']}
];

const state={map:null,L:null,layer:null,direct:null,reportLayer:null,reportPin:null,enabled:false,catId:'all',records:new Map(),follows:[],changes:{},timer:null,abort:null,pinMode:false,report:null,restoreLiveLayer:false,addressSuggestions:[],suggestTimer:null,suggestAbort:null,autoAddress:''};

function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function js(v){return String(v??'').replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/\r/g,'\\r').replace(/\n/g,'\\n');}
function trim(v){return v==null?'':String(v).trim();}
function cat(id){return CATEGORIES.find(c=>c.id===id)||null;}
function catOptions(includeAll){
  return (includeAll?[{id:'all',label:'All 311 types',types:[]}]:[]).concat(CATEGORIES)
    .map(c=>`<option value="${esc(c.id)}"${state.catId===c.id?' selected':''}>${esc(c.label)}</option>`).join('');
}
function sql(v){return String(v).replace(/'/g,"''");}
function inWhere(field,vals){return `${field} IN (${vals.map(v=>`'${sql(v)}'`).join(',')})`;}
function whereForCat(id){const c=cat(id);return c?inWhere('SRType',c.types):'1=1';}
function now(){return Date.now();}
function storeGet(k){try{return JSON.parse(sessionStorage.getItem(k)||'null');}catch(_){return null;}}
function storeSet(k,v){try{sessionStorage.setItem(k,JSON.stringify(v));}catch(_){}}
function loadFollows(){try{state.follows=JSON.parse(localStorage.getItem(FOLLOW_KEY)||'[]').slice(0,50);}catch(_){state.follows=[];}}
function saveFollows(){try{localStorage.setItem(FOLLOW_KEY,JSON.stringify(state.follows.slice(0,50)));}catch(_){}}
function srOk(sr){return /^\d{2}-\d{8}$/.test(String(sr||'').trim());}

function statusBucket(status){
  const s=trim(status), lo=s.toLowerCase();
  if(lo.includes('closed')){
    return /(duplicate|transferred|no access|unable|cancel|invalid)/.test(lo)?{bucket:'caveat',label:s||'Closed'}:{bucket:'closed',label:s||'Closed'};
  }
  return {bucket:'open',label:s||'Open'};
}
function fmtDate(ms){
  if(!ms)return 'Not posted';
  const d=new Date(Number(ms));
  return Number.isFinite(d.getTime())?d.toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'}):'Not posted';
}
function normalizeFeature(f){
  const a=f?.attributes||{}, geom=f?.geometry||{};
  const lat=Number(a.Latitude??geom.y), lng=Number(a.Longitude??geom.x);
  return {
    sr:trim(a.ServiceRequestNum),type:trim(a.SRType),status:trim(a.SRStatus),agency:trim(a.Agency),
    outcome:trim(a.Outcome),address:trim(a.Address),neighborhood:trim(a.Neighborhood),
    created:Number(a.CreatedDate)||null,due:Number(a.DueDate)||null,closed:Number(a.CloseDate)||null,lat,lng
  };
}
function parseArcgisResponse(json){
  const out=[], seen=new Set();
  (json?.features||[]).forEach(f=>{
    const r=normalizeFeature(f);
    if(!r.sr||seen.has(r.sr)||!Number.isFinite(r.lat)||!Number.isFinite(r.lng))return;
    seen.add(r.sr);out.push(r);
  });
  return out;
}
function timeoutSignal(parent,ms){
  const c=new AbortController(), t=setTimeout(()=>c.abort(),ms);
  if(parent)parent.addEventListener('abort',()=>c.abort(),{once:true});
  return {signal:c.signal,done:()=>clearTimeout(t)};
}
async function arcgis(params,signal){
  const t=timeoutSignal(signal,12000);
  const p=new URLSearchParams({f:'json',outFields:OUT,returnGeometry:'true',outSR:'4326',...params});
  try{
    const r=await fetch(`${API}?${p}`,{signal:t.signal});
    if(!r.ok)throw new Error(`311 ${r.status}`);
    const j=await r.json();
    if(j.error)throw new Error(j.error.message||'311 error');
    const records=parseArcgisResponse(j);
    records.limitHit=!!j.exceededTransferLimit||records.length>=LIMIT;
    return records;
  }finally{t.done();}
}
function bboxKey(b,catId){
  return ['live311',catId,b.getWest(),b.getSouth(),b.getEast(),b.getNorth()].map((v,i)=>i<2?v:Number(v).toFixed(3)).join(':');
}
function layerParams(){
  const b=state.map.getBounds();
  return {where:whereForCat(state.catId),geometry:[b.getWest(),b.getSouth(),b.getEast(),b.getNorth()].join(','),geometryType:'esriGeometryEnvelope',inSR:'4326',spatialRel:'esriSpatialRelIntersects',resultRecordCount:String(LIMIT)};
}
function setStatus(msg){const el=document.getElementById('live311-status');if(el)el.textContent=msg||'';}
function toast(msg){if(g.toast)g.toast(msg);else setStatus(msg);}

function init(opts){
  if(state.map)return api;
  state.map=opts&&opts.map;state.L=opts&&opts.L;
  if(!state.map||!state.L)return api;
  state.layer=state.L.layerGroup();
  state.direct=state.L.layerGroup().addTo(state.map);
  if(state.map.createPane&&!state.map.getPane?.('live311-report')){
    const pane=state.map.createPane('live311-report');
    if(pane?.style)pane.style.zIndex=760;
  }
  state.reportLayer=state.L.layerGroup().addTo(state.map);
  mountPanel();loadFollows();renderFollows();bindMap();refreshFollows();handleDeepLink();
  return api;
}
function bindMap(){
  state.map.on('moveend zoomend',schedule);
  state.map.on('click',e=>{
    if(!state.pinMode)return;
    setReportLocation(e.latlng);
  });
  const container=state.map.getContainer&&state.map.getContainer();
  if(container&&container.addEventListener)container.addEventListener('click',handlePinContainerClick,true);
}
function handlePinContainerClick(event){
  if(!state.pinMode||!state.map||!state.map.mouseEventToLatLng)return;
  const latlng=state.map.mouseEventToLatLng(event);
  if(!latlng)return;
  setReportLocation(latlng);
  if(event.preventDefault)event.preventDefault();
  if(event.stopPropagation)event.stopPropagation();
}
function setEnabled(on){
  state.enabled=!!on;
  if(state.enabled){state.layer.addTo(state.map);schedule();}
  else{if(state.abort)state.abort.abort();state.layer.clearLayers();if(state.map.hasLayer(state.layer))state.map.removeLayer(state.layer);setStatus('');}
}
function setCategory(id){state.catId=id||'all';schedule();}
function refresh(){schedule(0);}
function schedule(wait=400){clearTimeout(state.timer);state.timer=setTimeout(loadLayer,wait);}
async function loadLayer(){
  if(!state.enabled||!state.map)return;
  if(state.map.getZoom()<MIN_ZOOM){state.layer.clearLayers();setStatus(`Zoom to ${MIN_ZOOM}+ to load live 311 requests.`);return;}
  const b=state.map.getBounds(), key=bboxKey(b,state.catId), cached=storeGet(key);
  if(cached&&now()-cached.t<CACHE_TTL){renderLayer(cached.records,cached.limitHit);return;}
  if(state.abort)state.abort.abort();
  state.abort=new AbortController();
  setStatus('Loading live 311 requests...');
  try{
    const records=await arcgis(layerParams(),state.abort.signal);
    storeSet(key,{t:now(),records,limitHit:records.limitHit});
    renderLayer(records,records.limitHit);
  }catch(e){
    if(e.name==='AbortError')return;
    state.layer.clearLayers();setStatus('311 layer unavailable - will retry');
  }
}
function renderLayer(records,limitHit){
  state.layer.clearLayers();state.records=new Map();
  records.forEach(r=>{state.records.set(r.sr,r);marker(r).addTo(state.layer);});
  const c=cat(state.catId);
  setStatus(limitHit?`${records.length}+ requests here - zoom in or filter.`:`${records.length} live 311 request${records.length===1?'':'s'}${c?` - ${c.label}`:''}.`);
}
function marker(r){
  const b=statusBucket(r.status), color=COLORS[b.bucket];
  const ic=state.L.divIcon({className:'',html:`<div class="live311-dot" style="background:${color};border-color:var(--bg,#fff)"></div>`,iconSize:[14,14],iconAnchor:[7,7]});
  return state.L.marker([r.lat,r.lng],{icon:ic,title:`${r.sr} ${r.type}`}).bindPopup(popupHtml(r),{maxWidth:300});
}
function popupHtml(r){
  const b=statusBucket(r.status);
  return `<div class="live311-pop"><div class="live311-pop-title">${esc(r.type||'311 request')}</div><div class="live311-pop-sr">${esc(r.sr)}</div>
  <div class="live311-chip ${b.bucket}">${esc(b.label)}</div>${r.outcome?`<div class="live311-muted">${esc(r.outcome)}</div>`:''}
  <dl><dt>Agency</dt><dd>${esc(r.agency||'Not posted')}</dd><dt>Created</dt><dd>${fmtDate(r.created)}</dd><dt>Due</dt><dd>${fmtDate(r.due)}</dd><dt>Closed</dt><dd>${fmtDate(r.closed)}</dd><dt>Address</dt><dd>${esc(r.address||r.neighborhood||'Not posted')}</dd></dl>
  <div class="live311-actions"><button type="button" onclick="Live311.follow('${js(r.sr)}')">Follow</button><button type="button" onclick="Live311.copySrLink('${js(r.sr)}')">Copy link</button></div></div>`;
}

function mountPanel(){
  let panel=document.getElementById('live311-panel');
  if(!panel){
    const body=document.getElementById('side-body');
    if(!body)return;
    panel=document.createElement('div');panel.id='live311-panel';panel.className='pane live311-panel';
    body.insertBefore(panel,body.firstChild);
  }
  panel.innerHTML=`<div class="shd">Live 311</div><div class="live311-row"><select class="rfs" id="live311-cat" onchange="Live311.setCategory(this.value)" aria-label="Filter live 311 category">${catOptions(true)}</select><button type="button" class="ss-btn" onclick="Live311.refresh()">Refresh</button></div><div id="live311-status" class="live311-muted"></div><div class="shd">Followed requests</div><div class="live311-caption">Saved on this device only.</div><div id="live311-follows"></div>`;
}
function chip(status){const b=statusBucket(status);return `<span class="live311-chip ${b.bucket}">${esc(b.label)}</span>`;}
function renderFollows(){
  const el=document.getElementById('live311-follows');if(!el)return;
  if(!state.follows.length){el.innerHTML='<div class="live311-muted">No followed 311 requests yet.</div>';return;}
  el.innerHTML=state.follows.map(f=>`<div class="live311-follow"><div><button type="button" class="live311-link" onclick="Live311.openSr('${js(f.sr)}')">${esc(f.sr)}</button><div class="live311-muted">${esc(f.label||'311 request')}</div>${state.changes[f.sr]?`<div class="live311-change">${esc(state.changes[f.sr]).replace(' -&gt; ',' &rarr; ')}</div>`:''}</div><div>${chip(f.lastStatus)}<button type="button" class="pin-clear" onclick="Live311.unfollow('${js(f.sr)}')" aria-label="Unfollow ${esc(f.sr)}">x</button></div></div>`).join('')+`<button type="button" class="ss-btn" style="width:100%;margin-top:6px" onclick="Live311.refreshFollows()">Re-check follows</button>`;
}
function upsertFollow(list,rec,extra={},t=now()){
  const f={sr:rec.sr,label:rec.type||rec.address||extra.label||'',lastStatus:rec.status||extra.lastStatus||'',lastChecked:t,match_method:extra.match_method};
  const rest=list.filter(x=>x.sr!==f.sr);
  return [f,...rest].slice(0,50);
}
function mergeFollows(list,records,t=now()){
  const by=new Map(records.map(r=>[r.sr,r])), changes={};
  const follows=list.map(f=>{
    const r=by.get(f.sr);
    if(!r)return {...f,lastChecked:t};
    if(f.lastStatus&&r.status&&f.lastStatus!==r.status)changes[f.sr]=`was ${f.lastStatus} -> now ${r.status}`;
    return {...f,label:f.label||r.type,lastStatus:r.status||f.lastStatus,lastChecked:t};
  });
  return {follows,changes};
}
async function refreshFollows(){
  loadFollows();
  if(!state.follows.length){renderFollows();return;}
  try{
    const records=await arcgis({where:inWhere('ServiceRequestNum',state.follows.map(f=>f.sr)),resultRecordCount:String(state.follows.length)});
    const merged=mergeFollows(state.follows,records);
    state.follows=merged.follows;state.changes=merged.changes;saveFollows();renderFollows();
  }catch(_){renderFollows();setStatus('311 follow refresh unavailable - will retry');}
}
async function follow(sr){
  let rec=state.records.get(sr);
  if(!rec){const got=await queryExact(sr);rec=got[0];}
  if(!rec){toast('311 request not found.');return;}
  addFollowRecord(rec,{match_method:'follow_button'});
}
function addFollowRecord(rec,extra){
  state.follows=upsertFollow(state.follows,rec,extra);saveFollows();renderFollows();toast(`Following ${rec.sr}`);
}
function unfollow(sr){state.follows=state.follows.filter(f=>f.sr!==sr);delete state.changes[sr];saveFollows();renderFollows();}
async function queryExact(sr){if(!srOk(sr))return [];return arcgis({where:`ServiceRequestNum = '${sql(sr)}'`,resultRecordCount:'1'});}
async function openSr(sr){
  const records=await queryExact(sr), rec=records[0];
  if(!rec){toast('311 request not found.');return;}
  state.direct.clearLayers();
  const m=marker(rec).addTo(state.direct);
  state.map.setView([rec.lat,rec.lng],Math.max(state.map.getZoom(),16));
  m.openPopup();
}
function copySrLink(sr){
  const u=new URL(g.location.href);u.search=`?sr=${encodeURIComponent(sr)}`;u.hash='';
  copyText(String(u),'311 link copied.');
}
function copyText(text,msg){
  if(navigator.clipboard)navigator.clipboard.writeText(text).then(()=>toast(msg)).catch(()=>toast(text));
  else toast(text);
}
function handleDeepLink(){const sr=new URLSearchParams(g.location.search).get('sr');if(srOk(sr))openSr(sr);}

function ensureModal(){
  let m=document.getElementById('live311-modal');
  if(m)return m;
  m=document.createElement('div');m.id='live311-modal';m.className='live311-modal';m.setAttribute('aria-hidden','true');
  document.body.appendChild(m);return m;
}
function ensurePinGuide(){
  let el=document.getElementById('live311-pin-guide');
  if(el)return el;
  el=document.createElement('div');el.id='live311-pin-guide';el.className='live311-pin-guide';el.setAttribute('aria-hidden','true');
  (document.getElementById('map-wrap')||document.body).appendChild(el);
  return el;
}
function hideReportModal(){const m=ensureModal();m.classList.remove('open');m.setAttribute('aria-hidden','true');}
function setReportMapMode(on){
  document.getElementById('map-wrap')?.classList.toggle('live311-pinning',!!on);
  if(typeof g.CustomEvent==='function'&&g.dispatchEvent)g.dispatchEvent(new g.CustomEvent('live311:pinning',{detail:{on:!!on}}));
  if(on){
    state.restoreLiveLayer=!!(state.enabled&&state.layer&&state.map?.hasLayer?.(state.layer));
    if(state.restoreLiveLayer)state.map.removeLayer(state.layer);
  }else if(state.restoreLiveLayer&&state.enabled&&state.layer){
    state.layer.addTo(state.map);state.restoreLiveLayer=false;
  }
}
function hidePinGuide(){const el=document.getElementById('live311-pin-guide');if(el){el.classList.remove('open');el.setAttribute('aria-hidden','true');}setReportMapMode(false);}
function pinCoord(r){return `${Number(r.lat).toFixed(5)}, ${Number(r.lng).toFixed(5)}`;}
function addressQuery(address){return /\bbaltimore\b/i.test(address)?address:`${address}, Baltimore, MD`;}
function addressSuggestionItems(){return (state.addressSuggestions||[]).map((s,i)=>`<button type="button" onclick="Live311.chooseAddress(${i})">${esc(s.text)}</button>`).join('');}
function reportPinIcon(){
  return state.L.divIcon({className:'',html:'<div class="live311-report-pin" aria-hidden="true"></div>',iconSize:[24,24],iconAnchor:[12,22]});
}
function updateReportPin(latlng){
  if(!state.map||!state.L||!latlng)return;
  const lat=Number(latlng.lat), lng=Number(latlng.lng);
  if(!Number.isFinite(lat)||!Number.isFinite(lng))return;
  if(!state.reportLayer&&state.L.layerGroup)state.reportLayer=state.L.layerGroup().addTo(state.map);
  if(!state.reportPin){
    state.reportPin=state.L.marker([lat,lng],{icon:reportPinIcon(),draggable:true,title:'311 report location',pane:'live311-report'}).addTo(state.reportLayer||state.map);
    if(state.reportPin.on)state.reportPin.on('dragend',()=>{
      const p=state.reportPin.getLatLng&&state.reportPin.getLatLng();
      setReportLocation(p);
    });
  }else if(state.reportPin.setLatLng){
    state.reportPin.setLatLng([lat,lng]);
  }
}
function clearReportPin(){if(state.reportLayer&&state.reportLayer.clearLayers)state.reportLayer.clearLayers();state.reportPin=null;}
function setReportLocation(latlng,msg){
  const lat=Number(latlng?.lat), lng=Number(latlng?.lng);
  if(!Number.isFinite(lat)||!Number.isFinite(lng))return;
  const r=state.report=state.report||{step:1,catId:CATEGORIES[0].id,handoffTime:null,candidates:[]};
  r.lat=lat;r.lng=lng;r.msg=msg||'Map point selected. Add an address or landmark before generating the packet.';
  updateReportPin({lat,lng});
  showPinGuide();
  setStatus(`311 report pin selected at ${pinCoord(r)}.`);
}
function showPinGuide(){
  const r=state.report||{}, hasPin=Number.isFinite(r.lat)&&Number.isFinite(r.lng);
  const el=ensurePinGuide();el.classList.add('open');el.setAttribute('aria-hidden','false');
  setReportMapMode(true);
  el.innerHTML=`<div class="live311-pin-guide-head">Choose the 311 location</div><div class="live311-pin-guide-copy">${hasPin?`Pinned ${pinCoord(r)}. Drag the marker or click another map spot to adjust.`:'The report form is paused while you place the map pin.'}</div><div class="live311-pin-guide-steps">1. Click the exact spot on the map.<br>2. Press Use this point to return to the packet.</div><div class="live311-row"><button type="button" class="sb-btn primary" onclick="Live311.finishPin()"${hasPin?'':' disabled'}>Use this point</button><button type="button" class="sb-btn" onclick="Live311.cancelPin()">Back to form</button></div>`;
}
function openReport(){state.pinMode=false;hidePinGuide();clearReportPin();state.report={step:0,catId:CATEGORIES[0].id,handoffTime:null,candidates:[]};renderReport(0);}
function closeReport(){state.pinMode=false;hidePinGuide();clearReportPin();state.report=null;hideReportModal();setStatus('');}
function renderReport(step){
  const m=ensureModal(), r=state.report=state.report||{};r.step=step;
  m.classList.add('open');m.setAttribute('aria-hidden','false');
  if(step===0){m.innerHTML=`<div class="live311-box"><div class="sbtitle">Create a Baltimore 311 packet</div><div class="sbsub">Emergencies still need 911. For 311, this dashboard helps you copy the location, category, and notes into Baltimore's official portal.</div><div class="sb-row"><button type="button" class="sb-btn primary" onclick="Live311.reportNext()">Continue</button><button type="button" class="sb-btn" onclick="Live311.closeReport()">Close</button></div></div>`;return;}
  if(step===1){
    if(Number.isFinite(r.lat)&&Number.isFinite(r.lng))updateReportPin({lat:r.lat,lng:r.lng});
    const hasPin=Number.isFinite(r.lat)&&Number.isFinite(r.lng);
    m.innerHTML=`<div class="live311-box"><div class="sbtitle">311 report packet</div><div class="sbsub">Type an address to zoom to street level, or drop a pin manually.</div><div class="live311-grid"><button type="button" class="sb-btn" onclick="Live311.beginPin()">${hasPin?'Move pin on map':'Drop pin on map'}</button><button type="button" class="sb-btn" onclick="Live311.useGeo()">Use my location</button></div><div class="live311-pin-summary ${hasPin?'set':''}">${hasPin?`Map point: ${pinCoord(r)}`:'No map point selected yet.'}</div><label class="rfl" for="live311-address">Address / landmark</label><div class="live311-row"><div class="live311-address-wrap"><input class="rfs" id="live311-address" value="${esc(r.address||'')}" autocomplete="off" aria-autocomplete="list" aria-controls="live311-address-suggestions" oninput="Live311.queueAddressSuggest(this.value)" onkeydown="if(event.key==='Enter'){event.preventDefault();Live311.findAddress();}" required><div id="live311-address-suggestions" class="live311-suggestions">${addressSuggestionItems()}</div></div><button type="button" class="ss-btn" onclick="Live311.findAddress()">Find address</button></div><label class="rfl" for="live311-report-cat">Category</label><select class="rfs" id="live311-report-cat">${CATEGORIES.map(c=>`<option value="${esc(c.id)}"${(r.catId||'')===c.id?' selected':''}>${esc(c.label)}</option>`).join('')}</select><label class="rfl" for="live311-desc">Description</label><textarea class="rft" id="live311-desc" placeholder="Optional details for the portal packet">${esc(r.desc||'')}</textarea><div id="live311-modal-msg" class="live311-change">${esc(r.msg||'')}</div><div class="sb-row"><button type="button" class="sb-btn primary" onclick="Live311.generatePacket()">Generate packet</button><button type="button" class="sb-btn" onclick="Live311.closeReport()">Close</button></div></div>`;return;
  }
  const packet=packetText(r);
  m.innerHTML=`<div class="live311-box"><div class="sbtitle">Copy packet, then submit in Baltimore 311</div><div id="live311-packet">${esc(packet)}</div><div class="sb-row"><button type="button" class="sb-btn primary" onclick="Live311.copyPacket()">Copy packet</button><button type="button" class="sb-btn" onclick="Live311.openPortal()">Open Baltimore 311 portal</button></div><label class="rfl" for="live311-sr-input">SR number after submitting</label><div class="live311-row"><input class="rfs" id="live311-sr-input" placeholder="26-00000000"><button type="button" class="ss-btn" onclick="Live311.validatePasted()">Validate and follow</button></div><button type="button" class="ss-btn" onclick="Live311.findCandidates()">Find recent nearby requests</button><div id="live311-modal-msg" class="live311-change">${esc(r.msg||'')}</div><div class="live311-candidates">${candidateHtml(r.candidates||[])}</div><div class="sb-row"><button type="button" class="sb-btn" onclick="Live311.reportBack()">Back</button><button type="button" class="sb-btn" onclick="Live311.closeReport()">Close</button></div></div>`;
}
function saveReportInputs(){
  const r=state.report=state.report||{};
  r.address=trim(document.getElementById('live311-address')?.value);
  r.catId=document.getElementById('live311-report-cat')?.value||r.catId||CATEGORIES[0].id;
  r.desc=trim(document.getElementById('live311-desc')?.value);
}
function reportNext(){renderReport(1);}
function reportBack(){renderReport(1);}
function beginPin(){saveReportInputs();state.pinMode=true;hideReportModal();if(Number.isFinite(state.report?.lat)&&Number.isFinite(state.report?.lng))updateReportPin({lat:state.report.lat,lng:state.report.lng});showPinGuide();setStatus('Choose a 311 report location on the map.');}
function finishPin(){
  const r=state.report||{};
  if(!Number.isFinite(r.lat)||!Number.isFinite(r.lng)){showPinGuide();setStatus('Click the map to place the 311 report pin.');return;}
  state.pinMode=false;hidePinGuide();setStatus('');renderReport(1);
}
function cancelPin(){state.pinMode=false;hidePinGuide();setStatus('');renderReport(1);}
function isReportPinning(){return !!state.pinMode;}
async function suggestAddress(text,signal){
  const q=trim(text);
  if(q.length<3)return [];
  const p=new URLSearchParams({f:'json',text:addressQuery(q),category:'Address',maxSuggestions:'6',countryCode:'USA',searchExtent:'-76.75,39.18,-76.52,39.40'});
  const resp=await fetch(`${SUGGEST}?${p}`,{signal});
  if(!resp.ok)throw new Error(`Suggest ${resp.status}`);
  return ((await resp.json())?.suggestions||[]).filter(s=>s?.text).slice(0,6).map(s=>({text:s.text,magicKey:s.magicKey||''}));
}
function renderAddressSuggestions(){
  const list=document.getElementById('live311-address-suggestions');
  if(list)list.innerHTML=addressSuggestionItems();
}
function shouldAutoAddress(q){return q.length>=8&&/[a-z]/i.test(q);}
async function goToAddress(address,magicKey){
  const r=state.report=state.report||{};
  r.address=address;
  const input=document.getElementById('live311-address');if(input)input.value=address;
  try{
    const hit=await geocodeAddress(address,magicKey);
    if(!hit){r.msg='Address not found. Try a street number plus street name.';renderReport(1);return;}
    r.address=hit.label;state.pinMode=true;hideReportModal();
    state.map?.setView?.([hit.lat,hit.lng],REPORT_ZOOM);
    setReportLocation(hit,'Address found. Confirm the pin location, drag it, or click another spot.');
  }catch(_){
    r.msg='Address lookup is unavailable. Drop a pin on the map instead.';renderReport(1);
  }
}
async function chooseAddress(i){
  const s=(state.addressSuggestions||[])[Number(i)||0];
  if(!s)return findAddress();
  return goToAddress(s.text,s.magicKey);
}
function queueAddressSuggest(value){
  clearTimeout(state.suggestTimer);
  const q=trim(value);
  if(q.length<3){state.addressSuggestions=[];renderAddressSuggestions();return;}
  state.suggestTimer=setTimeout(async()=>{
    if(state.suggestAbort)state.suggestAbort.abort();
    const controller=new AbortController();state.suggestAbort=controller;
    try{
      const suggestions=await suggestAddress(q,controller.signal);
      if(trim(document.getElementById('live311-address')?.value)!==q)return;
      state.addressSuggestions=suggestions;renderAddressSuggestions();
      if(suggestions[0]&&shouldAutoAddress(q)&&state.autoAddress!==suggestions[0].text){
        state.autoAddress=suggestions[0].text;
        await goToAddress(suggestions[0].text,suggestions[0].magicKey);
      }
    }catch(_){}
  },650);
}
async function geocodeAddress(address,magicKey){
  const q=addressQuery(address);
  const p=new URLSearchParams({f:'json',SingleLine:q,outFields:'Match_addr,Score,Addr_type',maxLocations:'1',outSR:'4326',searchExtent:'-76.75,39.18,-76.52,39.40'});
  if(magicKey)p.set('magicKey',magicKey);
  const resp=await fetch(`${GEOCODE}?${p}`);
  if(!resp.ok)throw new Error(`Geocode ${resp.status}`);
  const hit=(await resp.json())?.candidates?.[0];
  const loc=hit?.location||{};
  const lat=Number(loc.y),lng=Number(loc.x);
  return Number.isFinite(lat)&&Number.isFinite(lng)?{lat,lng,label:hit.address||q}:null;
}
async function findAddress(){
  saveReportInputs();
  const r=state.report=state.report||{};
  if(!r.address){r.msg='Type an address or landmark first.';renderReport(1);return;}
  r.msg='Finding that address in Baltimore...';renderReport(1);
  const magicKey=(state.addressSuggestions||[]).find(s=>s.text===r.address)?.magicKey;
  return goToAddress(r.address,magicKey);
}
function useGeo(){
  saveReportInputs();
  if(!navigator.geolocation){state.report.msg='Location is not available in this browser.';renderReport(1);return;}
  navigator.geolocation.getCurrentPosition(p=>{state.report.lat=p.coords.latitude;state.report.lng=p.coords.longitude;state.report.msg='Current location selected. Add an address or landmark before generating the packet.';state.map?.setView?.([state.report.lat,state.report.lng],REPORT_ZOOM);updateReportPin({lat:state.report.lat,lng:state.report.lng});renderReport(1);},()=>{state.report.msg='Could not get your location.';renderReport(1);},{enableHighAccuracy:true,timeout:10000});
}
function generatePacket(){
  saveReportInputs();
  const r=state.report, c=cat(r.catId);
  if(!r.address){r.msg='Address or landmark is required.';renderReport(1);return;}
  if(!Number.isFinite(r.lat)||!Number.isFinite(r.lng)){findAddress();return;}
  if(!c){r.msg='Choose a category.';renderReport(1);return;}
  state.pinMode=false;hidePinGuide();
  r.handoffTime=r.handoffTime||now();r.candidates=[];r.msg='';renderReport(2);
}
function packetText(r){
  const c=cat(r.catId)||CATEGORIES[0];
  return [`Category: ${c.label}`,`Address / landmark: ${r.address||''}`,`Description: ${r.desc||''}`,`Location: ${Number(r.lat).toFixed(6)}, ${Number(r.lng).toFixed(6)}`].join('\n');
}
function copyPacket(){copyText(packetText(state.report),'Report packet copied.');}
function openPortal(){g.open(PORTAL,'_blank','noopener');state.report.handoffTime=state.report.handoffTime||now();}
async function validatePasted(){
  const sr=trim(document.getElementById('live311-sr-input')?.value), r=state.report;
  if(!srOk(sr)){r.msg='Enter an SR number like 26-00004290.';renderReport(2);return;}
  const rec=(await queryExact(sr))[0];
  if(!rec){r.msg='That SR number was not found.';renderReport(2);return;}
  addFollowRecord(rec,{match_method:'pasted'});r.msg=`Validated and followed ${rec.sr}.`;renderReport(2);
}
function bboxAround(lat,lng,m=250){
  const dLat=m/111320, dLng=m/(111320*Math.max(.2,Math.cos(lat*Math.PI/180)));
  return [lng-dLng,lat-dLat,lng+dLng,lat+dLat].join(',');
}
async function findCandidates(){
  const r=state.report, c=cat(r.catId);
  if(!Number.isFinite(r.lat)||!Number.isFinite(r.lng)||!c){r.msg='Packet location and category are required.';renderReport(2);return;}
  r.msg='Searching recent nearby 311 requests...';renderReport(2);
  try{
    const where=[whereForCat(r.catId),`CreatedDate >= ${Math.floor(r.handoffTime||now())}`].join(' AND ');
    const records=await arcgis({where,geometry:bboxAround(r.lat,r.lng),geometryType:'esriGeometryEnvelope',inSR:'4326',spatialRel:'esriSpatialRelIntersects',resultRecordCount:'20'});
    r.candidates=candidateMatches(records,r);r.msg=r.candidates.length?'Pick a matching request below.':'No recent nearby matches found.';renderReport(2);
  }catch(_){r.msg='Nearby search unavailable - try again after submitting.';renderReport(2);}
}
function candidateHtml(items){
  return items.map(x=>`<div class="live311-candidate"><div><strong>${esc(x.r.type)}</strong><div class="live311-muted">${esc(x.r.sr)} - ${esc(x.r.address||'No address')} - ${fmtDate(x.r.created)}</div>${chip(x.r.status)}</div><button type="button" class="ss-btn" onclick="Live311.confirmCandidate('${js(x.r.sr)}')">Follow</button></div>`).join('');
}
function confirmCandidate(sr){
  const item=(state.report.candidates||[]).find(x=>x.r.sr===sr);
  if(!item)return;
  addFollowRecord(item.r,{match_method:'confirmed_candidate'});state.report.msg=`Followed ${sr}.`;renderReport(2);
}
function hav(a,b,c,d){
  const R=6371000, toRad=x=>x*Math.PI/180, dLat=toRad(c-a), dLng=toRad(d-b);
  const s=Math.sin(dLat/2)**2+Math.cos(toRad(a))*Math.cos(toRad(c))*Math.sin(dLng/2)**2;
  return 2*R*Math.atan2(Math.sqrt(s),Math.sqrt(1-s));
}
function candidateMatches(records,draft){
  const c=cat(draft.catId), types=new Set(c?c.types:[]);
  return records.map(r=>({r,distanceM:hav(draft.lat,draft.lng,r.lat,r.lng)}))
    .filter(x=>x.distanceM<=250&&(!types.size||types.has(x.r.type))&&(!draft.handoffTime||x.r.created>=draft.handoffTime))
    .map(x=>({...x,score:Math.round(x.distanceM+Math.max(0,(x.r.created-(draft.handoffTime||x.r.created))/60000)*4+(statusBucket(x.r.status).bucket==='open'?0:20))}))
    .sort((a,b)=>a.score-b.score).slice(0,8);
}

const api={init,setEnabled,setCategory,refresh,refreshFollows,follow,unfollow,openSr,copySrLink,openReport,closeReport,reportNext,reportBack,beginPin,finishPin,cancelPin,isReportPinning,findAddress,chooseAddress,queueAddressSuggest,useGeo,generatePacket,copyPacket,openPortal,validatePasted,findCandidates,confirmCandidate,
  _test:{CATEGORIES,parseArcgisResponse,statusBucket,fmtDate,upsertFollow,mergeFollows,candidateMatches,packetText,whereForCat,suggestAddress,geocodeAddress}};
g.Live311=api;
if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
