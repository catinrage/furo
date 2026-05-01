const key='furo-client-panel-theme';
const root=document.documentElement;
const saved=localStorage.getItem(key)||'light';
root.dataset.theme=saved;
const themeButton=document.getElementById('theme');
if(themeButton){
  themeButton.onclick=()=>{
    const next=root.dataset.theme==='dark'?'light':'dark';
    root.dataset.theme=next;
    localStorage.setItem(key,next);
  };
}
