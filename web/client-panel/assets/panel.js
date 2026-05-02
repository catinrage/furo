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

const routeList=document.getElementById('route-list');
const routeCount=document.getElementById('routes-count');
const routeTemplate=document.getElementById('route-template');
const addRoute=document.getElementById('add-route');

function wireRoute(card){
  const remove=card.querySelector('[data-remove-route]');
  if(remove){
    remove.addEventListener('click',()=>{
      const deleteInput=card.querySelector('[data-delete]');
      if(deleteInput){
        deleteInput.value='1';
        card.classList.add('removed');
      }else{
        card.remove();
      }
    });
  }
}

document.querySelectorAll('[data-route]').forEach(wireRoute);

if(addRoute&&routeList&&routeCount&&routeTemplate){
  addRoute.addEventListener('click',()=>{
    const index=Number(routeCount.value||'0');
    const html=routeTemplate.innerHTML.replaceAll('__INDEX__',String(index));
    const wrapper=document.createElement('div');
    wrapper.innerHTML=html.trim();
    const card=wrapper.firstElementChild;
    routeList.appendChild(card);
    wireRoute(card);
    routeCount.value=String(index+1);
    const first=card.querySelector('input');
    if(first){first.focus();}
  });
}
