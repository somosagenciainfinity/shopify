from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import httpx
import asyncio
import json
import secrets
from datetime import datetime
import uvicorn
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Shopify Task Processor", version="3.0.0")

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

# ==================== MODELOS DE DADOS ====================
class TaskRequest(BaseModel):
    id: str
    productIds: List[str]
    operations: List[Dict[str, Any]]
    storeName: str
    accessToken: str
    taskType: Optional[str] = "bulk_edit"
    config: Optional[Dict[str, Any]] = {}
    workerUrl: Optional[str] = None

# ==================== ENDPOINTS PRINCIPAIS ====================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "🚀 Railway API - Gerenciamento Completo de Tarefas!",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "tasks_in_memory": len(tasks_db),
        "version": "3.0.0",
        "features": [
            "Processamento de tarefas",
            "Agendamento de tarefas",
            "Pausar/Retomar tarefas",
            "Cancelamento de tarefas",
            "Gerenciamento completo"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check detalhado"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": "running",
        "tasks": {
            "total": len(tasks_db),
            "scheduled": sum(1 for t in tasks_db.values() if t["status"] == "scheduled"),
            "processing": sum(1 for t in tasks_db.values() if t["status"] in ["processing", "running"]),
            "paused": sum(1 for t in tasks_db.values() if t["status"] == "paused"),
            "completed": sum(1 for t in tasks_db.values() if "completed" in t["status"]),
            "cancelled": sum(1 for t in tasks_db.values() if t["status"] == "cancelled")
        },
        "metrics": {
            "total_products_processed": sum(len(t.get("results", [])) for t in tasks_db.values()),
            "memory_usage_kb": len(str(tasks_db)) / 1024
        }
    }

# ==================== CRIAR E PROCESSAR TAREFAS ====================

@app.post("/process-task")
async def process_task(task: TaskRequest, background_tasks: BackgroundTasks):
    """Processar tarefa em background"""
    logger.info(f"📋 Nova tarefa {task.id}: {len(task.productIds)} produtos")
    
    # Validar dados
    if not task.productIds:
        raise HTTPException(status_code=400, detail="Nenhum produto para processar")
    
    if not task.operations:
        raise HTTPException(status_code=400, detail="Nenhuma operação definida")
    
    # Salvar tarefa na memória
    tasks_db[task.id] = {
        "id": task.id,
        "name": f"Edição em Massa - {len(task.productIds)} produtos",
        "status": "processing",
        "task_type": task.taskType,
        "progress": {
            "processed": 0,
            "total": len(task.productIds),
            "successful": 0,
            "failed": 0,
            "percentage": 0,
            "current_product": None
        },
        "started_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "config": task.dict(),
        "results": []
    }
    
    logger.info(f"✅ Tarefa {task.id} iniciada")
    
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

# ==================== AGENDAMENTO DE TAREFAS ====================

@app.post("/api/tasks/schedule")
async def schedule_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Criar nova tarefa agendada"""
    task_id = data.get("id") or f"task_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    # LOG PARA DEBUG
    logger.info(f"📋 Recebendo agendamento: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    # Verificar se o horário já passou
    scheduled_for = data.get("scheduled_for", datetime.now().isoformat())
    
    # CORREÇÃO: Tratar timezone - remover 'Z' e converter para timezone-naive
    if scheduled_for.endswith('Z'):
        scheduled_for_clean = scheduled_for[:-1] + '+00:00'
    else:
        scheduled_for_clean = scheduled_for
    
    try:
        scheduled_time = datetime.fromisoformat(scheduled_for_clean)
        # Remover timezone info para comparação com datetime.now()
        if scheduled_time.tzinfo is not None:
            scheduled_time = scheduled_time.replace(tzinfo=None)
    except:
        # Fallback para formato sem timezone
        scheduled_time = datetime.fromisoformat(scheduled_for.replace('Z', ''))
    
    now = datetime.now()
    
    # Se já passou, executar imediatamente
    if scheduled_time <= now:
        logger.info(f"📅 Tarefa {task_id} agendada para horário passado, executando imediatamente!")
        
        task = {
            "id": task_id,
            "name": data.get("name", "Tarefa Agendada"),
            "task_type": data.get("task_type", "bulk_edit"),
            "status": "processing",  # Já inicia processando
            "scheduled_for": scheduled_for,
            "started_at": now.isoformat(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        
        # Processar imediatamente
        config = task.get("config", {})
        background_tasks.add_task(
            process_products_background,
            task_id,
            config.get("productIds", []),
            config.get("operations", []),
            config.get("storeName", ""),
            config.get("accessToken", "")
        )
        
        logger.info(f"▶️ Tarefa {task_id} iniciada imediatamente")
    else:
        # Agendar normalmente
        task = {
            "id": task_id,
            "name": data.get("name", "Tarefa Agendada"),
            "task_type": data.get("task_type", "bulk_edit"),
            "status": "scheduled",
            "scheduled_for": scheduled_for,
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        logger.info(f"📅 Tarefa {task_id} agendada para {scheduled_for}")
        
        # LOG ADICIONAL
        diff = (scheduled_time - now).total_seconds()
        logger.info(f"⏱️ Tarefa será executada em {diff:.0f} segundos")
    
    return {
        "success": True,
        "taskId": task_id,
        "task": task
    }

@app.post("/api/tasks/execute/{task_id}")
async def execute_scheduled_task(task_id: str, background_tasks: BackgroundTasks):
    """Executar uma tarefa agendada imediatamente"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    if task["status"] != "scheduled":
        return {
            "success": False,
            "message": f"Tarefa não está agendada (status: {task['status']})"
        }
    
    # Mudar status para processing
    task["status"] = "processing"
    task["started_at"] = datetime.now().isoformat()
    task["updated_at"] = datetime.now().isoformat()
    
    # Extrair configurações
    config = task.get("config", {})
    
    # Processar em background
    background_tasks.add_task(
        process_products_background,
        task_id,
        config.get("productIds", []),
        config.get("operations", []),
        config.get("storeName", ""),
        config.get("accessToken", "")
    )
    
    logger.info(f"▶️ Tarefa agendada {task_id} iniciada manualmente")
    
    return {
        "success": True,
        "message": "Tarefa iniciada com sucesso",
        "task": task
    }

# ==================== PAUSAR E RETOMAR TAREFAS ====================

@app.post("/api/tasks/pause/{task_id}")
async def pause_task(task_id: str):
    """Pausar uma tarefa em execução"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    if task["status"] not in ["processing", "running"]:
        return {
            "success": False,
            "message": f"Tarefa não pode ser pausada (status: {task['status']})"
        }
    
    task["status"] = "paused"
    task["paused_at"] = datetime.now().isoformat()
    task["updated_at"] = datetime.now().isoformat()
    
    logger.info(f"⏸️ Tarefa {task_id} pausada")
    
    return {
        "success": True,
        "message": "Tarefa pausada com sucesso",
        "task": task
    }

@app.post("/api/tasks/resume/{task_id}")
async def resume_task(task_id: str, background_tasks: BackgroundTasks):
    """Retomar uma tarefa pausada"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    if task["status"] != "paused":
        return {
            "success": False,
            "message": f"Tarefa não está pausada (status: {task['status']})"
        }
    
    task["status"] = "processing"
    task["resumed_at"] = datetime.now().isoformat()
    task["updated_at"] = datetime.now().isoformat()
    
    # Continuar de onde parou
    config = task.get("config", {})
    product_ids = config.get("productIds", [])
    
    # Pegar apenas produtos não processados
    processed_count = task["progress"]["processed"]
    remaining_products = product_ids[processed_count:]
    
    if remaining_products:
        background_tasks.add_task(
            process_products_background,
            task_id,
            remaining_products,
            config.get("operations", []),
            config.get("storeName", ""),
            config.get("accessToken", ""),
            is_resume=True
        )
    
    logger.info(f"▶️ Tarefa {task_id} retomada")
    
    return {
        "success": True,
        "message": "Tarefa retomada com sucesso",
        "task": task
    }

# ==================== CANCELAR TAREFAS ====================

@app.post("/api/tasks/cancel/{task_id}")
async def cancel_task(task_id: str):
    """Cancelar uma tarefa (agendada, pausada ou em execução)"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    # Só não pode cancelar se já terminou
    if task["status"] in ["completed", "completed_with_errors", "failed"]:
        return {
            "success": False,
            "message": f"Tarefa já finalizada (status: {task['status']})"
        }
    
    task["status"] = "cancelled"
    task["cancelled_at"] = datetime.now().isoformat()
    task["updated_at"] = datetime.now().isoformat()
    
    logger.info(f"❌ Tarefa {task_id} cancelada")
    
    return {
        "success": True,
        "message": "Tarefa cancelada com sucesso",
        "task": task
    }

@app.post("/task-cancel/{task_id}")
async def cancel_task_alt(task_id: str):
    """Endpoint alternativo para cancelar tarefa (compatibilidade)"""
    return await cancel_task(task_id)

# ==================== LISTAR TAREFAS ====================

@app.get("/tasks")
async def list_tasks_simple():
    """Endpoint simples /tasks para compatibilidade"""
    tasks_list = list(tasks_db.values())
    
    return {
        "success": True,
        "tasks": tasks_list,
        "total": len(tasks_list)
    }

@app.get("/api/tasks/all")
async def get_all_tasks():
    """Retornar TODAS as tarefas com estatísticas"""
    all_tasks = list(tasks_db.values())
    
    # Ordenar por updated_at mais recente
    all_tasks.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    return {
        "success": True,
        "total": len(all_tasks),
        "tasks": all_tasks,
        "stats": {
            "scheduled": sum(1 for t in all_tasks if t["status"] == "scheduled"),
            "processing": sum(1 for t in all_tasks if t["status"] in ["processing", "running"]),
            "paused": sum(1 for t in all_tasks if t["status"] == "paused"),
            "completed": sum(1 for t in all_tasks if t["status"] == "completed"),
            "completed_with_errors": sum(1 for t in all_tasks if t["status"] == "completed_with_errors"),
            "failed": sum(1 for t in all_tasks if t["status"] == "failed"),
            "cancelled": sum(1 for t in all_tasks if t["status"] == "cancelled")
        }
    }

@app.get("/api/tasks/scheduled")
async def get_scheduled_tasks():
    """Retornar APENAS tarefas agendadas"""
    scheduled_tasks = []
    
    for task_id, task in tasks_db.items():
        if task.get("status") == "scheduled":
            scheduled_tasks.append(task)
    
    # Ordenar por data de agendamento
    scheduled_tasks.sort(key=lambda x: x.get("scheduled_for", ""))
    
    logger.info(f"📅 Retornando {len(scheduled_tasks)} tarefas agendadas")
    
    return {
        "success": True,
        "total": len(scheduled_tasks),
        "tasks": scheduled_tasks
    }

@app.get("/api/tasks/running")
async def get_running_tasks():
    """Retornar tarefas em execução e pausadas"""
    active_tasks = []
    
    for task_id, task in tasks_db.items():
        if task.get("status") in ["processing", "running", "paused"]:
            active_tasks.append(task)
    
    # Ordenar por progresso
    active_tasks.sort(key=lambda x: x.get("progress", {}).get("percentage", 0))
    
    logger.info(f"🏃 Retornando {len(active_tasks)} tarefas ativas")
    
    return {
        "success": True,
        "total": len(active_tasks),
        "tasks": active_tasks
    }

# ==================== STATUS E ATUALIZAÇÃO ====================

@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Verificar status detalhado da tarefa"""
    
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
    logger.info(f"📊 Status: {task['status']} - {task['progress']['percentage']}%")
    
    return task

@app.put("/api/tasks/update/{task_id}")
async def update_task(task_id: str, data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Atualizar qualquer tarefa"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    # LOG PARA DEBUG
    if "scheduled_for" in data:
        old_time = task.get("scheduled_for")
        new_time = data["scheduled_for"]
        logger.info(f"📅 Mudando horário da tarefa {task_id}")
        logger.info(f"   De: {old_time}")
        logger.info(f"   Para: {new_time}")
    
    # Atualizar campos permitidos
    updatable_fields = ["name", "scheduled_for", "priority", "description", "status"]
    for field in updatable_fields:
        if field in data:
            task[field] = data[field]
    
    task["updated_at"] = datetime.now().isoformat()
    
    # IMPORTANTE: Se atualizou o scheduled_for e já passou, executar IMEDIATAMENTE
    if "scheduled_for" in data and task["status"] == "scheduled":
        scheduled_for = data["scheduled_for"]
        
        # CORREÇÃO: Tratar timezone
        if scheduled_for.endswith('Z'):
            scheduled_for_clean = scheduled_for[:-1] + '+00:00'
        else:
            scheduled_for_clean = scheduled_for
        
        try:
            scheduled_time = datetime.fromisoformat(scheduled_for_clean)
            # Remover timezone info para comparação
            if scheduled_time.tzinfo is not None:
                scheduled_time = scheduled_time.replace(tzinfo=None)
        except:
            # Fallback
            scheduled_time = datetime.fromisoformat(scheduled_for.replace('Z', ''))
        
        now = datetime.now()
        
        if scheduled_time <= now:
            logger.info(f"📝 Tarefa {task_id} atualizada para horário passado, executando imediatamente!")
            
            # Mudar status e processar
            task["status"] = "processing"
            task["started_at"] = now.isoformat()
            
            config = task.get("config", {})
            
            # Processar em background
            background_tasks.add_task(
                process_products_background,
                task_id,
                config.get("productIds", []),
                config.get("operations", []),
                config.get("storeName", ""),
                config.get("accessToken", "")
            )
            
            logger.info(f"▶️ Tarefa {task_id} iniciada após edição")
    else:
        logger.info(f"📝 Tarefa {task_id} atualizada")
    
    return {
        "success": True,
        "task": task
    }

# ==================== DELETAR TAREFAS ====================

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str):
    """Deletar uma tarefa"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    del tasks_db[task_id]
    
    logger.info(f"🗑️ Tarefa {task_id} deletada")
    
    return {
        "success": True,
        "message": "Tarefa deletada com sucesso",
        "deleted_task": task
    }

@app.delete("/tasks/clear")
async def clear_all_tasks():
    """Limpar todas as tarefas da memória"""
    count = len(tasks_db)
    tasks_db.clear()
    
    logger.info(f"🗑️ {count} tarefas removidas da memória")
    
    return {
        "success": True,
        "message": f"{count} tarefas removidas",
        "timestamp": datetime.now().isoformat()
    }

# ==================== PROCESSAMENTO DE PRODUTOS ====================

async def process_products_background(
    task_id: str, 
    product_ids: List[str], 
    operations: List[Dict], 
    store_name: str,
    access_token: str,
    is_resume: bool = False
):
    """PROCESSAR PRODUTOS EM BACKGROUND"""
    if not is_resume:
        logger.info(f"🚀 INICIANDO PROCESSAMENTO: {task_id}")
    else:
        logger.info(f"▶️ RETOMANDO PROCESSAMENTO: {task_id}")
    
    logger.info(f"📦 Produtos para processar: {len(product_ids)}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    # Se for retomada, pegar progresso existente
    if is_resume and task_id in tasks_db:
        task = tasks_db[task_id]
        processed = task["progress"]["processed"]
        successful = task["progress"]["successful"]
        failed = task["progress"]["failed"]
        results = task.get("results", [])
        total = task["progress"]["total"]
    else:
        processed = 0
        successful = 0
        failed = 0
        results = []
        total = len(product_ids)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            try:
                # Verificar status da tarefa
                if task_id in tasks_db:
                    current_status = tasks_db[task_id].get("status")
                    
                    # Se foi pausada, parar
                    if current_status == "paused":
                        logger.info(f"⏸️ Tarefa {task_id} pausada, parando processamento")
                        return
                    
                    # Se foi cancelada, parar
                    if current_status == "cancelled":
                        logger.info(f"❌ Tarefa {task_id} cancelada, parando processamento")
                        return
                
                logger.info(f"📦 Processando produto {product_id} ({i+1}/{len(product_ids)})")
                
                # URL da API
                product_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
                
                # Buscar produto
                get_response = await client.get(product_url, headers=headers)
                
                if get_response.status_code != 200:
                    raise Exception(f"Erro ao buscar: {get_response.status_code}")
                
                product_data = get_response.json()
                current_product = product_data.get("product", {})
                product_title = current_product.get("title", "Sem título")
                
                # Preparar atualização
                update_payload = {"product": {"id": int(product_id)}}
                
                # CORREÇÃO: Coletar todas as operações de variantes primeiro
                variant_updates = {}
                for variant in current_product.get("variants", []):
                    variant_updates[variant["id"]] = {"id": variant["id"]}
                
                # Aplicar operações
                for op in operations:
                    field = op.get("field")
                    value = op.get("value")
                    
                    logger.info(f"  Aplicando: {field} = {value}")
                    
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
                    
                    # CORREÇÃO: Acumular updates de variantes
                    elif field in ["price", "compare_at_price", "sku"]:
                        for variant_id in variant_updates:
                            if field == "price":
                                variant_updates[variant_id]["price"] = str(value)
                            elif field == "compare_at_price":
                                variant_updates[variant_id]["compare_at_price"] = str(value) if value else None
                            elif field == "sku":
                                variant_updates[variant_id]["sku"] = str(value)
                
                # Adicionar variantes ao payload apenas uma vez com TODOS os campos
                if variant_updates:
                    update_payload["product"]["variants"] = list(variant_updates.values())
                    logger.info(f"  Atualizando {len(variant_updates)} variantes")
                
                # Log do payload final
                logger.info(f"  Payload final: {json.dumps(update_payload, indent=2)}")
                
                # Enviar atualização
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
                        "message": "Produto atualizado com sucesso"
                    }
                    logger.info(f"✅ Produto {product_id} atualizado")
                else:
                    failed += 1
                    error_text = await update_response.text()
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "failed",
                        "message": f"Erro HTTP {update_response.status_code}: {error_text}"
                    }
                    logger.error(f"❌ Erro no produto {product_id}: {error_text}")
                    
            except Exception as e:
                failed += 1
                result = {
                    "product_id": product_id,
                    "status": "failed",
                    "message": str(e)
                }
                logger.error(f"❌ Exceção: {str(e)}")
            
            # Atualizar progresso
            results.append(result)
            processed += 1
            percentage = round((processed / total) * 100)
            
            # Salvar na memória
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
                tasks_db[task_id]["results"] = results[-50:]
            
            # Rate limiting
            await asyncio.sleep(0.3)
    
    # Finalizar
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
        tasks_db[task_id]["results"] = results
        
        logger.info(f"🏁 TAREFA FINALIZADA: ✅ {successful} | ❌ {failed}")

# ==================== VERIFICADOR DE TAREFAS AGENDADAS ====================

async def check_and_execute_scheduled_tasks():
    """Verificar e executar tarefas agendadas automaticamente"""
    while True:
        try:
            now = datetime.now()
            
            for task_id, task in list(tasks_db.items()):
                if task["status"] == "scheduled":
                    scheduled_for = task["scheduled_for"]
                    
                    # CORREÇÃO: Tratar timezone
                    if scheduled_for.endswith('Z'):
                        scheduled_for_clean = scheduled_for[:-1] + '+00:00'
                    else:
                        scheduled_for_clean = scheduled_for
                    
                    try:
                        scheduled_time = datetime.fromisoformat(scheduled_for_clean)
                        # Remover timezone info para comparação
                        if scheduled_time.tzinfo is not None:
                            scheduled_time = scheduled_time.replace(tzinfo=None)
                    except:
                        # Fallback
                        scheduled_time = datetime.fromisoformat(scheduled_for.replace('Z', ''))
                    
                    # Se já passou do horário, executar imediatamente
                    if scheduled_time <= now:
                        logger.info(f"⏰ Executando tarefa agendada {task_id}")
                        
                        # Mudar status e processar
                        task["status"] = "processing"
                        task["started_at"] = now.isoformat()
                        task["updated_at"] = now.isoformat()
                        
                        config = task.get("config", {})
                        
                        # Criar task para processar
                        asyncio.create_task(
                            process_products_background(
                                task_id,
                                config.get("productIds", []),
                                config.get("operations", []),
                                config.get("storeName", ""),
                                config.get("accessToken", "")
                            )
                        )
            
            # Verificar a cada 20 segundos
            await asyncio.sleep(20)
            
        except Exception as e:
            logger.error(f"Erro no verificador de tarefas: {e}")
            await asyncio.sleep(20)

# Iniciar verificador de tarefas agendadas quando o servidor iniciar
@app.on_event("startup")
async def startup_event():
    """Iniciar verificador de tarefas agendadas"""
    asyncio.create_task(check_and_execute_scheduled_tasks())
    logger.info("⏰ Verificador de tarefas agendadas iniciado")

if __name__ == "__main__":
    port = 8000
    logger.info(f"🚀 Railway Shopify Processor v3.0 iniciado na porta {port}")
    logger.info(f"✅ Sistema completo de gerenciamento de tarefas ativo!")
    logger.info(f"📋 Funcionalidades: Agendar, Processar, Pausar, Retomar, Cancelar")
    uvicorn.run(app, host="0.0.0.0", port=port)