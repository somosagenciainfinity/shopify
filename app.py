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

# CORS - IMPORTANTE!
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
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

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "🚀 Railway API - Processando tarefas!",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "tasks_in_memory": len(tasks_db),
        "version": "2.0.0"
    }

@app.post("/process-task")
async def process_task(task: TaskRequest, background_tasks: BackgroundTasks):
    """Processar tarefa em background"""
    logger.info(f"📋 Nova tarefa {task.id}: {len(task.productIds)} produtos")
    logger.info(f"🔧 Operações: {task.operations}")
    logger.info(f"🏪 Loja: {task.storeName}")
    
    # Validar dados
    if not task.productIds:
        raise HTTPException(status_code=400, detail="Nenhum produto para processar")
    
    if not task.operations:
        raise HTTPException(status_code=400, detail="Nenhuma operação definida")
    
    # Salvar tarefa na memória COM STATUS INICIAL
    tasks_db[task.id] = {
        "id": task.id,
        "status": "processing",  # ← IMPORTANTE: começar como processing
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
    
    logger.info(f"✅ Tarefa {task.id} salva na memória")
    
    # Processar em background
    background_tasks.add_task(
        process_products_background,
        task.id,
        task.productIds,
        task.operations,
        task.storeName,
        task.accessToken
    )
    
    return {
        "success": True,
        "message": f"Processamento iniciado para {len(task.productIds)} produtos",
        "taskId": task.id,
        "estimatedTime": f"{len(task.productIds) * 0.3:.1f} segundos",
        "mode": "background_processing"
    }

async def process_products_background(
    task_id: str, 
    product_ids: List[str], 
    operations: List[Dict], 
    store_name: str,
    access_token: str
):
    """PROCESSAR PRODUTOS EM BACKGROUND"""
    logger.info(f"🚀 INICIANDO PROCESSAMENTO REAL: {task_id}")
    logger.info(f"📦 Total de produtos: {len(product_ids)}")
    logger.info(f"⚙️ Operações a aplicar: {operations}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    total = len(product_ids)
    processed = 0
    successful = 0
    failed = 0
    results = []
    
    # Atualizar status inicial
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = "processing"
        tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            try:
                # Verificar cancelamento
                if task_id in tasks_db and tasks_db[task_id].get("status") == "cancelled":
                    logger.info(f"⏹️ Tarefa {task_id} cancelada")
                    return
                
                logger.info(f"📦 [{i+1}/{total}] Processando produto ID: {product_id}")
                
                # URL da API REST do Shopify
                product_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
                
                # 1. BUSCAR PRODUTO ATUAL
                logger.info(f"🔍 Buscando produto {product_id}...")
                get_response = await client.get(product_url, headers=headers)
                
                if get_response.status_code != 200:
                    logger.error(f"❌ Erro ao buscar produto: {get_response.status_code}")
                    logger.error(f"Resposta: {get_response.text}")
                    raise Exception(f"Erro ao buscar produto: {get_response.status_code}")
                
                product_data = get_response.json()
                current_product = product_data.get("product", {})
                product_title = current_product.get("title", "Sem título")
                
                logger.info(f"✅ Produto encontrado: {product_title}")
                
                # 2. PREPARAR PAYLOAD DE ATUALIZAÇÃO
                update_payload = {"product": {"id": int(product_id)}}
                
                # Aplicar cada operação
                for op in operations:
                    field = op.get("field")
                    value = op.get("value")
                    
                    logger.info(f"🔧 Aplicando: {field} = {value}")
                    
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
                        if isinstance(value, list):
                            new_tags = value
                        else:
                            new_tags = [t.strip() for t in str(value).split(',') if t.strip()]
                        
                        if op.get("meta", {}).get("mode") == "replace":
                            update_payload["product"]["tags"] = ", ".join(new_tags)
                        else:
                            current_tags = current_product.get("tags", "").split(',')
                            current_tags = [t.strip() for t in current_tags if t.strip()]
                            all_tags = list(set(current_tags + new_tags))
                            update_payload["product"]["tags"] = ", ".join(all_tags)
                    
                    # Variantes (preço, SKU, etc)
                    elif field in ["price", "compare_at_price", "sku"] and current_product.get("variants"):
                        if "variants" not in update_payload["product"]:
                            update_payload["product"]["variants"] = []
                        
                        for variant in current_product["variants"]:
                            variant_update = {"id": variant["id"]}
                            
                            if field == "price":
                                variant_update["price"] = str(value)
                            elif field == "compare_at_price":
                                variant_update["compare_at_price"] = str(value) if value else None
                            elif field == "sku":
                                variant_update["sku"] = str(value)
                            
                            update_payload["product"]["variants"].append(variant_update)
                
                logger.info(f"📤 Enviando atualização: {json.dumps(update_payload, indent=2)}")
                
                # 3. ENVIAR ATUALIZAÇÃO
                update_response = await client.put(
                    product_url,
                    headers=headers,
                    json=update_payload
                )
                
                # 4. PROCESSAR RESULTADO
                if update_response.status_code == 200:
                    successful += 1
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "success",
                        "message": "Produto atualizado com sucesso"
                    }
                    logger.info(f"✅ SUCESSO: Produto {product_id} ({product_title}) atualizado!")
                else:
                    failed += 1
                    error_detail = update_response.text[:500]
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "failed",
                        "message": f"Erro HTTP {update_response.status_code}",
                        "error": error_detail
                    }
                    logger.error(f"❌ FALHA: Produto {product_id}: {update_response.status_code}")
                    logger.error(f"Detalhes: {error_detail}")
                    
            except Exception as e:
                failed += 1
                result = {
                    "product_id": product_id,
                    "status": "failed",
                    "message": str(e)
                }
                logger.error(f"❌ EXCEÇÃO ao processar {product_id}: {str(e)}")
            
            # 5. ATUALIZAR PROGRESSO
            results.append(result)
            processed = i + 1
            percentage = round((processed / total) * 100)
            
            # ATUALIZAR NA MEMÓRIA (IMPORTANTE!)
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "percentage": percentage,
                    "current_product": product_title
                }
                tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
                tasks_db[task_id]["results"] = results[-50:]  # Últimos 50
                
                logger.info(f"📊 PROGRESSO SALVO: {processed}/{total} ({percentage}%)")
            
            # Rate limiting
            await asyncio.sleep(0.3)
    
    # 6. FINALIZAR TAREFA
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
        tasks_db[task_id]["results"] = results
        
        logger.info(f"🏁 TAREFA FINALIZADA: {final_status}")
        logger.info(f"📊 RESULTADO FINAL: ✅ {successful} sucessos | ❌ {failed} falhas")

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Verificar status da tarefa"""
    logger.info(f"📊 Status solicitado para: {task_id}")
    
    if task_id not in tasks_db:
        logger.warning(f"⚠️ Tarefa {task_id} não encontrada")
        return {
            "id": task_id,
            "status": "not_found",
            "message": "Tarefa não encontrada",
            "progress": {
                "processed": 0,
                "total": 0,
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
    
    task = tasks_db[task_id]
    logger.info(f"📊 Retornando status: {task['status']} - {task['progress']['percentage']}%")
    
    return task

@app.get("/health")
async def health_check():
    """Health check"""
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
            "total_products_processed": sum(len(t.get("results", [])) for t in tasks_db.values()),
            "memory_usage_kb": len(str(tasks_db)) / 1024
        }
    }

if __name__ == "__main__":
    port = 8000
    logger.info(f"🚀 Railway Shopify Processor iniciado na porta {port}")
    logger.info(f"✅ Pronto para processar edições em massa!")
    uvicorn.run(app, host="0.0.0.0", port=port)