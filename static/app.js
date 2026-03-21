/* DBS Check — app.js (NextStep SaaS) */
"use strict";

const $ = id => document.getElementById(id);
function setText(id, msg) { const el=$(id); if(el) el.textContent=msg||""; }
function setDisabled(id, v) { const el=$(id); if(el) el.disabled=!!v; }
function escapeHtml(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
    .replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;");
}
function toast(msg,pos){
  pos=pos||"top-right"; const id=pos==="top-right"?"toastTR":"toastBR";
  let host=document.getElementById(id);
  if(!host){host=document.createElement("div");host.id=id;host.className=`toastHost ${pos}`;document.body.appendChild(host);}
  const t=document.createElement("div");t.className="toast";t.textContent=msg;host.appendChild(t);
  setTimeout(()=>t.classList.add("show"),10);
  setTimeout(()=>{t.classList.remove("show");t.classList.add("hide");},3500);
  setTimeout(()=>{try{t.remove();}catch(e){}},4200);
}

// ── State ─────────────────────────────────────────────────────────────────────
let bulkFiles=[]; let lastJobId=null; let pollTimer=null;
function stopPolling(){if(pollTimer){clearTimeout(pollTimer);pollTimer=null;}}
function updateBulkCount(){
  setText("bulkCount",`${bulkFiles.length}/100 files selected`);
  const rows=document.querySelectorAll("#bulkList .bulkRow").length;
  setText("bulkRowCount",rows?`${rows}/100 rows ready`:"");
}

// ── File chips ────────────────────────────────────────────────────────────────
function renderChips(){
  const wrap=$("bulkChips");if(!wrap)return;wrap.innerHTML="";
  bulkFiles.forEach((f,idx)=>{
    const chip=document.createElement("span");chip.className="chipFile";
    chip.innerHTML=`<span title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>`;
    const btn=document.createElement("button");btn.type="button";btn.textContent="×";
    btn.addEventListener("click",()=>{bulkFiles.splice(idx,1);renderChips();updateBulkCount();});
    chip.appendChild(btn);wrap.appendChild(chip);
  });
}
function isSupportedFile(f){
  const n=(f?.name||"").toLowerCase();
  return n.endsWith(".pdf")||n.endsWith(".png")||n.endsWith(".jpg")||n.endsWith(".jpeg")||
         n.endsWith(".webp")||n.endsWith(".csv")||n.endsWith(".xlsx")||n.endsWith(".docx");
}
function appendFiles(files){
  let rej=0;
  Array.from(files||[]).forEach(f=>{
    if(!isSupportedFile(f)){rej++;return;}
    if(bulkFiles.length>=100)return;
    bulkFiles.push(f);
  });
  renderChips();updateBulkCount();
  if(rej) setText("extractBulkStatus",`Skipped ${rej} unsupported file(s).`);
}

// ── Drop zone ─────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded",()=>{
  const dz=$("dropZone");const inp=$("files");
  if(dz&&inp){
    dz.addEventListener("click",()=>inp.click());
    dz.addEventListener("dragover",e=>{e.preventDefault();dz.classList.add("dragover");});
    dz.addEventListener("dragleave",()=>dz.classList.remove("dragover"));
    dz.addEventListener("drop",e=>{e.preventDefault();dz.classList.remove("dragover");appendFiles(e.dataTransfer.files);});
  }
  inp?.addEventListener("change",e=>{appendFiles(e.target.files);e.target.value="";});
  $("btnAddMore")?.addEventListener("click",()=>$("files")?.click());
  $("btnClearAll")?.addEventListener("click",()=>{
    bulkFiles=[];renderChips();updateBulkCount();
    const list=$("bulkList");if(list)list.innerHTML="";
    setText("extractBulkStatus","");setText("runBulkStatus","");setText("zipNotice","");
    _resetZipBtn();setDisabled("btnDlResultsXlsx",true);setDisabled("btnDlResultsCsv",true);
    _hideProgress();_hideRerun();lastJobId=null;stopPolling();
  });
  updateBulkCount();setDisabled("btnDlResultsXlsx",true);setDisabled("btnDlResultsCsv",true);
});

// ── Badge ─────────────────────────────────────────────────────────────────────
function buildBadge(status){
  const span=document.createElement("span");
  const map={queued:"badge",running:"badge running",clear:"badge clear",
             needs_review:"badge needs_review",portal_unavailable:"badge portal_unavailable",
             failed:"badge portal_unavailable"};
  const labels={queued:"Queued",running:"Running…",clear:"Clear",
                needs_review:"Needs Review",portal_unavailable:"Portal Unavailable",failed:"Failed"};
  span.className=map[status]||"badge";span.textContent=labels[status]||status;return span;
}

// ── Render bulk table ─────────────────────────────────────────────────────────
function renderBulkTable(items){
  const list=$("bulkList");if(!list)return;list.innerHTML="";
  (items||[]).forEach((it,idx)=>{
    const row=document.createElement("div");
    row.className="bulkRow";row.dataset.row=String(idx+1);
    row.dataset.originalFilename=it.original_filename||"";
    row.dataset.dirty="false";
    const dob=it.dob_day&&it.dob_month&&it.dob_year
      ?`${String(it.dob_day).padStart(2,"0")}/${String(it.dob_month).padStart(2,"0")}/${it.dob_year}`:"";
    const certConf=it.confidence?.certificate_number||0;
    const snConf=it.confidence?.surname||0;
    const dobConf=it.confidence?.dob||0;
    function dot(c){return `<span class="confDot ${c>=80?"confHigh":c>=50?"confMed":"confLow"}"></span>`;}
    row.innerHTML=`
      <div class="bulkRowTop">
        <div class="bulkRowLeft">
          <button type="button" class="iconBtn btnRemoveRow" data-idx="${idx}" title="Remove">
            <svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM7 9h2v9H7V9z"/></svg>
          </button>
          <div class="bulkIndex">#${idx+1}</div>
        </div>
        <div class="bulkFields">
          <div class="fieldBlock">
            <div class="fieldLabel">Source</div>
            <div class="bulkSource" title="${escapeHtml(it.original_filename||"")}">${escapeHtml(it.original_filename||"")}</div>
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">Certificate ${dot(certConf)}<span class="fieldHint">${certConf}%</span></div>
            <input class="cell cert" value="${escapeHtml(it.certificate_number||"")}" placeholder="Certificate No">
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">Surname ${dot(snConf)}<span class="fieldHint">${snConf}%</span></div>
            <div class="fieldRow">
              <input class="cell surname" value="${escapeHtml(it.surname||"")}" placeholder="Surname">
            </div>
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">DOB ${dot(dobConf)}<span class="fieldHint">${dobConf}%</span></div>
            <div class="fieldRow">
              <input class="cell dob-dd" style="width:52px" value="${escapeHtml(it.dob_day||"")}" placeholder="DD">
              <input class="cell dob-mm" style="width:52px" value="${escapeHtml(it.dob_month||"")}" placeholder="MM">
              <input class="cell dob-yy" style="width:70px" value="${escapeHtml(it.dob_year||"")}" placeholder="YYYY">
            </div>
          </div>
        </div>
        <div class="bulkActions">
          <div class="statusWrap"><span class="statusCell"></span></div>
          <div class="dlWrap dlCell"><span class="dlCell"></span></div>
        </div>
      </div>`;
    // Mark dirty on any field edit
    row.querySelectorAll("input.cell").forEach(inp=>{
      inp.addEventListener("input",()=>{row.dataset.dirty="true";_showRerunIfNeeded();});
    });
    list.appendChild(row);
  });
  updateBulkCount();
}

// ── Remove row ────────────────────────────────────────────────────────────────
document.addEventListener("click",e=>{
  const btn=e.target.closest?.(".btnRemoveRow");if(!btn)return;
  const row=btn.closest(".bulkRow");if(!row)return;
  const name=(row.dataset.originalFilename||"").trim();row.remove();
  if(name){const i=bulkFiles.findIndex(f=>(f?.name||"")===name);if(i>=0){bulkFiles.splice(i,1);renderChips();}}
  Array.from(document.querySelectorAll("#bulkList .bulkRow")).forEach((r,i)=>{
    r.dataset.row=String(i+1);const e=r.querySelector(".bulkIndex");if(e)e.textContent=`#${i+1}`;
  });
  updateBulkCount();
});

// ── Extract ───────────────────────────────────────────────────────────────────
$("btnExtractBulk")?.addEventListener("click",async()=>{
  if(!bulkFiles.length){setText("extractBulkStatus","Please add files first.");return;}
  setText("extractBulkStatus","Extracting…");setDisabled("btnExtractBulk",true);
  const fd=new FormData();bulkFiles.slice(0,100).forEach(f=>fd.append("files",f));
  try{
    const resp=await fetch("/dbs/extract",{method:"POST",body:fd});
    const data=await resp.json();
    if(!resp.ok)throw new Error(data?.detail||"Extraction failed");
    renderBulkTable(data.items||[]);
    updateBulkCount();
    setText("extractBulkStatus",`Done — ${(data.items||[]).length} row(s) extracted.`);
    if(data.notice)setText("extractBulkStatus",data.notice);
    toast("Extraction complete","top-right");
  }catch(err){setText("extractBulkStatus",err?.message||"Extraction failed.");}
  finally{setDisabled("btnExtractBulk",false);}
});

// ── Collect items ─────────────────────────────────────────────────────────────
function collectItems(dirtyOnly=false){
  return Array.from(document.querySelectorAll("#bulkList .bulkRow"))
    .filter(row=>!dirtyOnly||row.dataset.dirty==="true")
    .map(row=>({
      certificate_number:(row.querySelector("input.cert")?.value||"").trim(),
      surname:(row.querySelector("input.surname")?.value||"").trim().toUpperCase(),
      dob_day:(row.querySelector("input.dob-dd")?.value||"").trim(),
      dob_month:(row.querySelector("input.dob-mm")?.value||"").trim(),
      dob_year:(row.querySelector("input.dob-yy")?.value||"").trim(),
      original_filename:row.dataset.originalFilename||"",
      dirty:row.dataset.dirty==="true",
      row_number:parseInt(row.dataset.row||"0",10),
    }));
}

// ── Progress ──────────────────────────────────────────────────────────────────
function _showProgress(done,total,state,msg){
  const wrap=$("progressWrap");if(!wrap)return;wrap.classList.remove("hidden");
  setText("progressCount",`${done} of ${total} complete`);
  const pct=total>0?Math.round((done/total)*100):0;
  const bar=$("progressBar");if(bar)bar.style.width=pct+"%";
  const pmsg=$("progressMsg");if(pmsg&&msg)pmsg.textContent=msg;
  const badge=$("progressBadge");if(!badge)return;
  if(state==="queued"){badge.className="badge queued";badge.textContent="⏳ Queued";}
  else if(state==="running"){badge.className="badge running";badge.textContent="⚙️ Processing";}
  else if(state==="done"){badge.className="badge done";badge.textContent="✓ Complete";}
  else{badge.className="badge failed-all";badge.textContent="✗ Failed";}
}
function _hideProgress(){const w=$("progressWrap");if(w)w.classList.add("hidden");}

// ── ZIP helpers ───────────────────────────────────────────────────────────────
function _resetZipBtn(){
  const z=$("btnDlZip");if(!z)return;
  z.classList.add("disabledLink");z.removeAttribute("href");z.download="";
  z.textContent="⬇ Download All PDFs (ZIP)";
}
function _enableZipBtn(url,name){
  const z=$("btnDlZip");if(!z)return;
  z.href=url;z.download=name||"DBS_Checks.zip";z.classList.remove("disabledLink");
}

// ── Rerun helpers ─────────────────────────────────────────────────────────────
function _showRerunIfNeeded(){
  const hasDirty=Array.from(document.querySelectorAll("#bulkList .bulkRow")).some(r=>r.dataset.dirty==="true");
  const btn=$("btnRerun");if(!btn)return;
  if(hasDirty&&lastJobId)btn.classList.remove("hidden");else btn.classList.add("hidden");
}
function _hideRerun(){const b=$("btnRerun");if(b)b.classList.add("hidden");}
function _clearDirtyFlags(){document.querySelectorAll("#bulkList .bulkRow").forEach(r=>r.dataset.dirty="false");}

// ── Update UI from poll ───────────────────────────────────────────────────────
function updateBulkUIFromStatus(data,isRerun=false){
  const done=(data.running||{}).done||0;const total=(data.running||{}).total||0;
  _showProgress(done,total,data.state||"running",data.message||"");

  (data.rows||[]).forEach((r,idx)=>{
    const rowNum=r.row||idx+1;
    const tr=document.querySelector(`#bulkList .bulkRow[data-row="${rowNum}"]`);
    if(!tr)return;
    if(isRerun&&tr.dataset.dirty==="false"&&r.status!=="queued")return;
    const st=(r.status||"").trim();
    const sc=tr.querySelector(".statusCell");const dc=tr.querySelector(".dlCell");
    if(sc){
      sc.innerHTML="";sc.appendChild(buildBadge(st));
      if(st==="running"){const b=document.createElement("div");b.className="miniProgress";b.textContent="██████░░░░";sc.appendChild(b);}
      if((st==="needs_review"||st==="failed")&&r.error){
        const e=document.createElement("div");e.className="small";e.style.color="rgba(245,158,11,.9)";e.style.marginTop="4px";e.textContent=r.error;sc.appendChild(e);
      }
    }
    if(!dc)return;
    if(st==="running"||st==="queued"){dc.innerHTML=`<div class="miniSpinner"></div>`;return;}
    if(r.pdf_url){
      const lbl=st==="portal_unavailable"||st==="failed"?"⬇ Error Log":"⬇ Download PDF";
      const sty=(st==="portal_unavailable"||st==="failed")?`style="border-color:rgba(239,68,68,.35);"`:
                 st==="needs_review"?`style="border-color:rgba(245,158,11,.35);"`:""
      dc.innerHTML=`<a class="btnSmall downloadBtn" href="${escapeHtml(r.pdf_url)}" ${sty}>${lbl}</a>`;
    }else{dc.innerHTML="";}
  });

  setText("zipNotice",data.message||"");
  if(data.zip_ready&&data.zip_url)_enableZipBtn(data.zip_url,data.zip_name);

  const anyDone=(data.rows||[]).some(r=>r.status&&r.status!=="queued"&&r.status!=="running");
  setDisabled("btnDlResultsXlsx",!anyDone);setDisabled("btnDlResultsCsv",!anyDone);

  if(data.state==="done"){
    stopPolling();setDisabled("btnRunBulk",false);
    const runBtn=$("btnRunBulk");
    if(runBtn){runBtn.classList.remove("isRunning");runBtn.textContent="Run All Checks";}
    setText("runBulkStatus","");
    toast(`Run complete (${done}/${total})`,"bottom-right");
    _showRerunIfNeeded();
  }
}

// ── Poll ──────────────────────────────────────────────────────────────────────
function startPolling(jobId,isRerun=false){
  stopPolling();
  pollTimer=setTimeout(async function poll(){
    try{
      const r=await fetch(`/dbs/status/${jobId}`);const st=await r.json();
      if(!r.ok)throw new Error(st?.detail||"Status failed");
      updateBulkUIFromStatus(st,isRerun);
      if(st.state!=="done")pollTimer=setTimeout(poll,2000);
    }catch(e){setText("runBulkStatus","Updating…");pollTimer=setTimeout(poll,3000);}
  },1500);
}

// ── Run ───────────────────────────────────────────────────────────────────────
$("btnRunBulk")?.addEventListener("click",async()=>{
  stopPolling();_resetZipBtn();_hideRerun();_clearDirtyFlags();
  setText("runBulkStatus","");setDisabled("btnRunBulk",true);
  const runBtn=$("btnRunBulk");
  if(runBtn){runBtn.classList.add("isRunning");runBtn.textContent="Running…";}

  const org=($(  "org")?.value||"").trim();
  const fn=($("forename")?.value||"").trim();
  const sn=($("surname_user")?.value||"").trim();
  if(!org||!fn||!sn){
    setText("runBulkStatus","Please fill in Organisation, Forename and Surname (Step 1).");
    setDisabled("btnRunBulk",false);if(runBtn){runBtn.classList.remove("isRunning");runBtn.textContent="Run All Checks";}
    return;
  }

  const items=collectItems();
  if(!items.length||!items.some(it=>it.certificate_number)){
    setText("runBulkStatus","No items found. Please extract first.");
    setDisabled("btnRunBulk",false);if(runBtn){runBtn.classList.remove("isRunning");runBtn.textContent="Run All Checks";}
    return;
  }

  try{
    const resp=await fetch("/dbs/run",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({organisation_name:org,employee_forename:fn,employee_surname:sn,items})});
    const data=await resp.json();
    if(!resp.ok)throw new Error(data?.detail||"Run failed");
    lastJobId=data.job_id;
    updateBulkUIFromStatus({rows:data.rows||[],running:{done:0,total:(data.rows||[]).length},state:"queued",message:"Job queued…"});
    startPolling(lastJobId,false);
  }catch(err){
    setText("runBulkStatus",err?.message||"Run failed.");
    setDisabled("btnRunBulk",false);if(runBtn){runBtn.classList.remove("isRunning");runBtn.textContent="Run All Checks";}
  }
});

// ── Rerun ─────────────────────────────────────────────────────────────────────
document.addEventListener("click",async e=>{
  const btn=e.target.closest?.("#btnRerun");if(!btn)return;
  if(!lastJobId){toast("Please run a check first.","top-right");return;}
  const dirtyItems=collectItems(true);
  if(!dirtyItems.length){toast("No edited rows to rerun.","top-right");return;}

  stopPolling();btn.classList.add("hidden");
  setText("runBulkStatus","Rerunning edited rows…");

  dirtyItems.forEach(it=>{
    const row=document.querySelector(`#bulkList .bulkRow[data-row="${it.row_number}"]`);
    if(!row)return;
    const sc=row.querySelector(".statusCell");if(sc){sc.innerHTML="";sc.appendChild(buildBadge("queued"));}
    const dc=row.querySelector(".dlCell");if(dc)dc.innerHTML=`<div class="miniSpinner"></div>`;
  });

  const org=($("org")?.value||"").trim();
  const fn=($("forename")?.value||"").trim();
  const sn=($("surname_user")?.value||"").trim();

  try{
    const resp=await fetch("/dbs/rerun",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({job_id:lastJobId,items:dirtyItems,
                           organisation_name:org,employee_forename:fn,employee_surname:sn})});
    const data=await resp.json();
    if(!resp.ok)throw new Error(data?.detail||"Rerun failed");
    lastJobId=data.job_id;
    startPolling(lastJobId,true);
  }catch(err){
    setText("runBulkStatus",err?.message||"Rerun failed.");
    _showRerunIfNeeded();
  }
});

// ── Exports ───────────────────────────────────────────────────────────────────
$("btnDlResultsXlsx")?.addEventListener("click",async()=>{
  if(!lastJobId)return;
  const state=await fetch(`/dbs/status/${lastJobId}`).then(r=>r.json()).catch(()=>({}));
  const rows=state.rows||[];
  const checked_date=state.checked_date||"";
  await fetch("/dbs/export/results",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({format:"xlsx",rows,checked_date})})
    .then(async r=>{if(!r.ok)return;const b=await r.blob();const u=URL.createObjectURL(b);const a=document.createElement("a");a.href=u;a.download="DBS_Results.xlsx";a.click();URL.revokeObjectURL(u);});
});
$("btnDlResultsCsv")?.addEventListener("click",async()=>{
  if(!lastJobId)return;
  const state=await fetch(`/dbs/status/${lastJobId}`).then(r=>r.json()).catch(()=>({}));
  const rows=state.rows||[];
  const checked_date=state.checked_date||"";
  await fetch("/dbs/export/results",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({format:"csv",rows,checked_date})})
    .then(async r=>{if(!r.ok)return;const b=await r.blob();const u=URL.createObjectURL(b);const a=document.createElement("a");a.href=u;a.download="DBS_Results.csv";a.click();URL.revokeObjectURL(u);});
});
