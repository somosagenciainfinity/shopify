from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import httpx
import asyncio
import json
from datetime import datetime
import uvicorn
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Shopify Task Processor", version="2.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Armazenar tarefas em memória
tasks_db = {}

# Modelo de dados
class TaskRequest(BaseModel):
    id: str
    productIds: List[str]
    operations: List[Dict[str, Any]]
    storeName: str
    accessToken: str
    taskType: Optional[str] = "bulk_edit"
    config: Optional[Dict[str, Any]] = {}
    workerUrl: Optional[str] = None

class TaskUpdate(BaseModel):
    taskId: str
    status: str
    progress: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    results: Optional[List[Dict[str, Any]]] = None

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "🚀 Python API - Processando TODAS as tarefas!",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "tasks_in_memory": len(tasks_db),
        "version": "2.0.0"
    }

@app.post("/process-task")
async def process_task(task: TaskRequest, background_tasks: BackgroundTasks):
    """Processar QUALQUER tarefa em background - Curta ou longa!"""
    logger.info(f"📋 Nova tarefa {task.id}: {len(task.productIds)} produtos")
    
    # Salvar tarefa na memória
    tasks_db[task.id] = {
        "id": task.id,
        "status": "processing",
        "progress": {
            "processed": 0,
            "total": len(task.productIds),
            "successful": 0,
            "failed": 0,
            "percentage": 0,
            "current_product": None
        },
        "started_at": datetime.now().isoformat(),
        "config": task.dict(),
        "results": []
    }
    
    # Processar em background - TODAS AS TAREFAS!
    background_tasks.add_task(
        process_products_background,
        task.id,
        task.productIds,
        task.operations,
        task.storeName,
        task.accessToken,
        task.workerUrl
    )
    
    return {
        "success": True,
        "message": f"Processamento iniciado para {len(task.productIds)} produtos",
        "taskId": task.id,
        "estimatedTime": f"{len(task.productIds) * 0.3:.1f} segundos",
        "mode": "background_processing"
    }

async def update_worker_realtime(worker_url: str, task_id: str, update_data: dict):
    """Atualizar Worker EM TEMPO REAL - A CADA PRODUTO!"""
    if not worker_url:
        logger.warning("Worker URL não fornecida")
        return False
        
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"{worker_url}/api/tasks/railway-update",
                json={
                    "taskId": task_id,
                    "timestamp": datetime.now().isoformat(),
                    **update_data
                }
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Worker atualizado: Tarefa {task_id}")
                return True
            else:
                logger.error(f"❌ Erro ao atualizar Worker: {response.status_code}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Erro conectando com Worker: {e}")
        return False

async def process_products_background(task_id: str, product_ids: List[str], 
                                     operations: List[Dict], store_name: str,
                                     access_token: str, worker_url: str = None):
    """PROCESSAR PRODUTOS - ATUALIZA A CADA UM!"""
    logger.info(f"🚀 Iniciando processamento: {task_id}")
    logger.info(f"📦 Total de produtos: {len(product_ids)}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    total = len(product_ids)
    processed = 0
    successful = 0
    failed = 0
    results = []
    
    # Notificar início
    await update_worker_realtime(
        worker_url, task_id, {
            "status": "processing",
            "message": "Processamento iniciado",
            "progress": {
                "processed": 0,
                "total": total,
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
    )
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            try:
                # Verificar cancelamento
                if task_id in tasks_db and tasks_db[task_id].get("status") == "cancelled":
                    logger.info(f"⏹️ Tarefa {task_id} cancelada")
                    await update_worker_realtime(
                        worker_url, task_id, {
                            "status": "cancelled",
                            "message": "Tarefa cancelada pelo usuário",
                            "progress": tasks_db[task_id]["progress"]
                        }
                    )
                    return
                
                logger.info(f"📦 [{i+1}/{total}] Processando produto: {product_id}")
                
                # NOTIFICAR INÍCIO DO PRODUTO
                await update_worker_realtime(
                    worker_url, task_id, {
                        "status": "processing",
                        "currentAction": "fetching",
                        "currentProduct": {
                            "id": product_id,
                            "index": i + 1
                        }
                    }
                )
                
                # Buscar produto
                product_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
                
                # GET produto
                get_response = await client.get(product_url, headers=headers)
                
                if get_response.status_code != 200:
                    raise Exception(f"Erro ao buscar: {get_response.status_code}")
                
                product_data = get_response.json()
                current_product = product_data.get("product", {})
                product_title = current_product.get("title", "Sem título")
                
                # NOTIFICAR QUE ESTÁ ATUALIZANDO
                await update_worker_realtime(
                    worker_url, task_id, {
                        "status": "processing",
                        "currentAction": "updating",
                        "currentProduct": {
                            "id": product_id,
                            "title": product_title,
                            "index": i + 1
                        }
                    }
                )
                
                # Preparar atualização
                update_payload = {"product": {"id": product_id}}
                variant_updates = {}
                has_variant_update = False
                
                # Aplicar operações
                for op in operations:
                    field = op.get("field")
                    value = op.get("value")
                    
                    if field == "title":
                        update_payload["product"]["title"] = value
                    elif field in ["description", "body_html"]:
                        update_payload["product"]["body_html"] = value
                    elif field == "vendor":
                        update_payload["product"]["vendor"] = value
                    elif field == "product_type":
                        update_payload["product"]["product_type"] = value
                    elif field == "status":
                        update_payload["product"]["status"] = value
                    elif field == "tags":
                        new_tags = value.split(',') if value else []
                        new_tags = [tag.strip() for tag in new_tags if tag.strip()]
                        
                        if op.get("meta", {}).get("mode") == "replace":
                            update_payload["product"]["tags"] = ", ".join(new_tags)
                        else:
                            current_tags = current_product.get("tags", "").split(',')
                            current_tags = [tag.strip() for tag in current_tags if tag.strip()]
                            all_tags = list(set(current_tags + new_tags))
                            update_payload["product"]["tags"] = ", ".join(all_tags)
                    elif field in ["price", "compare_at_price", "sku"]:
                        has_variant_update = True
                        variant_updates[field] = value
                
                # Atualizar variantes se necessário
                if has_variant_update and current_product.get("variants"):
                    update_payload["product"]["variants"] = []
                    for variant in current_product["variants"]:
                        variant_update = {"id": variant["id"]}
                        
                        if "price" in variant_updates:
                            variant_update["price"] = str(variant_updates["price"])
                        if "compare_at_price" in variant_updates:
                            variant_update["compare_at_price"] = str(variant_updates["compare_at_price"])
                        if "sku" in variant_updates:
                            variant_update["sku"] = variant_updates["sku"]
                        
                        update_payload["product"]["variants"].append(variant_update)
                
                # PUT atualização
                update_response = await client.put(
                    product_url,
                    headers=headers,
                    json=update_payload
                )
                
                # Processar resultado
                if update_response.status_code == 200:
                    successful += 1
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "success",
                        "message": "Produto atualizado com sucesso",
                        "timestamp": datetime.now().isoformat()
                    }
                    logger.info(f"✅ Produto {product_id} atualizado")
                else:
                    failed += 1
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "failed",
                        "message": f"Erro HTTP {update_response.status_code}",
                        "error": update_response.text[:200],
                        "timestamp": datetime.now().isoformat()
                    }
                    logger.error(f"❌ Erro no produto {product_id}: {update_response.status_code}")
                    
            except Exception as e:
                failed += 1
                result = {
                    "product_id": product_id,
                    "status": "failed",
                    "message": str(e)[:200],
                    "timestamp": datetime.now().isoformat()
                }
                logger.error(f"❌ Erro processando {product_id}: {e}")
            
            # Adicionar resultado
            results.append(result)
            processed = i + 1
            percentage = round((processed / total) * 100)
            
            # Atualizar progresso na memória
            progress = {
                "processed": processed,
                "total": total,
                "successful": successful,
                "failed": failed,
                "percentage": percentage,
                "current_product": result.get("product_title", "")
            }
            
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = progress
                tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
                tasks_db[task_id]["results"] = results[-50:]  # Últimos 50 resultados
            
            # ⚡ ATUALIZAR WORKER A CADA PRODUTO! ⚡
            await update_worker_realtime(
                worker_url, task_id, {
                    "status": "processing",
                    "progress": progress,
                    "lastResult": result,
                    "message": f"Processado {processed}/{total} produtos"
                }
            )
            
            logger.info(f"📊 Progresso: {processed}/{total} ({percentage}%) - ✅ {successful} ❌ {failed}")
            
            # Rate limiting para não sobrecarregar Shopify
            await asyncio.sleep(0.2)
    
    # FINALIZAR TAREFA
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
        tasks_db[task_id]["results"] = results
    
    # Notificar conclusão
    await update_worker_realtime(
        worker_url, task_id, {
            "status": final_status,
            "progress": {
                "processed": processed,
                "total": total,
                "successful": successful,
                "failed": failed,
                "percentage": 100
            },
            "message": f"Processamento concluído: {successful} sucessos, {failed} falhas",
            "summary": {
                "total_processed": processed,
                "successful": successful,
                "failed": failed,
                "duration": datetime.now().isoformat()
            }
        }
    )
    
    logger.info(f"🎉 Tarefa {task_id} CONCLUÍDA! ✅ {successful} | ❌ {failed}")

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Verificar status detalhado da tarefa"""
    if task_id not in tasks_db:
        return {
            "id": task_id,
            "status": "not_found",
            "message": "Tarefa não encontrada"
        }
    
    task = tasks_db[task_id]
    return {
        **task,
        "active": task["status"] == "processing",
        "can_cancel": task["status"] == "processing"
    }

@app.get("/tasks")
async def list_tasks():
    """Listar todas as tarefas com estatísticas"""
    tasks = list(tasks_db.values())
    
    return {
        "total": len(tasks),
        "stats": {
            "processing": sum(1 for t in tasks if t["status"] == "processing"),
            "completed": sum(1 for t in tasks if "completed" in t["status"]),
            "cancelled": sum(1 for t in tasks if t["status"] == "cancelled"),
            "failed": sum(1 for t in tasks if t["status"] == "failed")
        },
        "tasks": tasks
    }

@app.post("/task-cancel/{task_id}")
async def cancel_task(task_id: str):
    """Cancelar uma tarefa em andamento"""
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    
    if tasks_db[task_id]["status"] != "processing":
        return {
            "success": False,
            "message": f"Tarefa não pode ser cancelada. Status: {tasks_db[task_id]['status']}"
        }
    
    tasks_db[task_id]["status"] = "cancelled"
    tasks_db[task_id]["cancelled_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": f"Tarefa {task_id} marcada para cancelamento",
        "task": tasks_db[task_id]
    }

@app.delete("/tasks/clear")
async def clear_all_tasks():
    """Limpar todas as tarefas da memória"""
    count = len(tasks_db)
    tasks_db.clear()
    
    return {
        "success": True,
        "message": f"{count} tarefas removidas",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check com métricas"""
    total_results = sum(len(t.get("results", [])) for t in tasks_db.values())
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": "running",
        "tasks": {
            "total": len(tasks_db),
            "processing": sum(1 for t in tasks_db.values() if t["status"] == "processing"),
            "completed": sum(1 for t in tasks_db.values() if "completed" in t["status"]),
            "cancelled": sum(1 for t in tasks_db.values() if t["status"] == "cancelled")
        },
        "metrics": {
            "total_products_processed": total_results,
            "memory_usage_kb": len(str(tasks_db)) / 1024
        }
    }

if __name__ == "__main__":
    port = 8000
    logger.info(f"🚀 Servidor iniciado na porta {port}")
    logger.info(f"📦 Pronto para processar TODAS as tarefas!")
    logger.info(f"⚡ Atualizações em TEMPO REAL a cada produto!")
    uvicorn.run(app, host="0.0.0.0", port=port)
