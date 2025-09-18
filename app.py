from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
import json
from datetime import datetime
from typing import Dict, List
import uvicorn

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Armazenar tarefas em memória (ou use Redis/PostgreSQL)
tasks_db = {}

@app.get("/")
async def root():
    return {"message": "Python API rodando sem limites!"}

@app.post("/process-task")
async def process_task(task_data: Dict, background_tasks: BackgroundTasks):
    """Processar tarefa em background - SEM LIMITE DE TEMPO!"""
    task_id = task_data.get("id", "default")
    
    # Salvar tarefa
    tasks_db[task_id] = {
        "id": task_id,
        "status": "processing",
        "progress": 0,
        "total": len(task_data.get("productIds", [])),
        "started_at": datetime.now().isoformat()
    }
    
    # Processar em background (SEM LIMITE!)
    background_tasks.add_task(process_products_background, task_id, task_data)
    
    return {
        "success": True,
        "message": "Processamento iniciado",
        "taskId": task_id
    }

async def process_products_background(task_id: str, task_data: Dict):
    """PROCESSAR PRODUTOS - PODE LEVAR HORAS!"""
    print(f"🚀 Processando tarefa {task_id}")
    
    product_ids = task_data.get("productIds", [])
    operations = task_data.get("operations", [])
    store_name = task_data.get("storeName", "")
    access_token = task_data.get("accessToken", "")
    
    total = len(product_ids)
    processed = 0
    successful = 0
    failed = 0
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            try:
                # Buscar produto do Shopify
                url = f"https://{store_name}.myshopify.com/admin/api/2024-04/products/{product_id}.json"
                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
                
                # Buscar produto
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    product = response.json()["product"]
                    
                    # Aplicar operações
                    update_data = {"product": {"id": product_id}}
                    
                    for op in operations:
                        field = op.get("field")
                        value = op.get("value")
                        
                        if field == "title":
                            update_data["product"]["title"] = value
                        elif field == "vendor":
                            update_data["product"]["vendor"] = value
                        elif field == "product_type":
                            update_data["product"]["product_type"] = value
                        # Adicione mais campos conforme necessário
                    
                    # Atualizar produto
                    update_response = await client.put(url, headers=headers, json=update_data)
                    
                    if update_response.status_code == 200:
                        successful += 1
                        print(f"✅ Produto {product_id} atualizado")
                    else:
                        failed += 1
                        print(f"❌ Erro ao atualizar produto {product_id}")
                else:
                    failed += 1
                    print(f"❌ Erro ao buscar produto {product_id}")
                    
            except Exception as e:
                print(f"❌ Erro: {e}")
                failed += 1
            
            processed = i + 1
            
            # Atualizar progresso
            tasks_db[task_id] = {
                "id": task_id,
                "status": "processing",
                "progress": processed,
                "total": total,
                "successful": successful,
                "failed": failed,
                "percentage": round((processed / total) * 100),
                "updated_at": datetime.now().isoformat()
            }
            
            print(f"📊 Progresso: {processed}/{total} ({tasks_db[task_id]['percentage']}%)")
            
            # Rate limiting (5 requests por segundo)
            await asyncio.sleep(0.2)
    
    # Marcar como completa
    tasks_db[task_id]["status"] = "completed"
    tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
    
    print(f"🎉 Tarefa {task_id} concluída! {successful} sucessos, {failed} falhas")

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Verificar status da tarefa"""
    if task_id in tasks_db:
        return tasks_db[task_id]
    return {"error": "Tarefa não encontrada"}

@app.get("/tasks")
async def list_tasks():
    """Listar todas as tarefas"""
    return list(tasks_db.values())

if __name__ == "__main__":
    # Rodar servidor
    uvicorn.run(app, host="0.0.0.0", port=8000)
