from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import httpx
import asyncio
import json
import secrets
from datetime import datetime, timezone, timedelta
import pytz
import uvicorn
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurar timezone de Brasília
BRAZIL_TZ = pytz.timezone('America/Sao_Paulo')

def get_brazil_time():
    """Retorna o horário atual de Brasília"""
    return datetime.now(BRAZIL_TZ)

def get_brazil_time_str():
    """Retorna o horário atual de Brasília como string ISO"""
    return get_brazil_time().isoformat()

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
        "timestamp": get_brazil_time_str(),
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
        "timestamp": get_brazil_time_str(),
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
        "started_at": get_brazil_time_str(),
        "updated_at": get_brazil_time_str(),
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

# ==================== PROCESSAMENTO DE VARIANTES VIA CSV ====================

@app.post("/process-variants-csv")
async def process_variants_csv(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Processar variantes usando CSV - compatível com o frontend de Variants"""
    
    task_id = data.get("id") or f"variant_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    logger.info(f"📋 Nova tarefa de variantes {task_id}")
    
    # Extrair dados do payload
    csv_content = data.get("csvContent", "")
    product_ids = data.get("productIds", [])
    submit_data = data.get("submitData", {})
    store_name = data.get("storeName", "")
    access_token = data.get("accessToken", "")
    
    if not csv_content:
        raise HTTPException(status_code=400, detail="CSV content não fornecido")
    
    # Salvar tarefa na memória
    tasks_db[task_id] = {
        "id": task_id,
        "name": f"Gerenciamento de Variantes - {len(product_ids)} produtos",
        "status": "processing",
        "task_type": "variant_management",
        "progress": {
            "processed": 0,
            "total": len(product_ids),
            "successful": 0,
            "failed": 0,
            "percentage": 0,
            "current_product": None
        },
        "started_at": get_brazil_time_str(),
        "updated_at": get_brazil_time_str(),
        "config": {
            "productIds": product_ids,
            "submitData": submit_data,
            "storeName": store_name,
            "hasCSV": True
        },
        "results": []
    }
    
    logger.info(f"✅ Tarefa de variantes {task_id} iniciada")
    
    # Processar em background
    background_tasks.add_task(
        process_variants_background,
        task_id,
        csv_content,
        product_ids,
        submit_data,
        store_name,
        access_token
    )
    
    return {
        "success": True,
        "message": f"Processamento de variantes iniciado para {len(product_ids)} produtos",
        "taskId": task_id,
        "mode": "csv_processing"
    }

async def process_variants_background(
    task_id: str,
    csv_content: str,
    product_ids: List[str],
    submit_data: Dict,
    store_name: str,
    access_token: str
):
    """Processar variantes em background usando CSV"""
    
    logger.info(f"🚀 INICIANDO PROCESSAMENTO DE VARIANTES: {task_id}")
    logger.info(f"📦 Produtos para processar: {len(product_ids)}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    processed = 0
    successful = 0
    failed = 0
    results = []
    total = len(product_ids)
    
    try:
        # Para cada produto, aplicar as mudanças via API
        for i, product_id in enumerate(product_ids):
            # Verificar se a tarefa foi pausada ou cancelada
            if task_id not in tasks_db:
                logger.warning(f"⚠️ Tarefa {task_id} não existe mais")
                return
            
            current_status = tasks_db[task_id].get("status")
            
            if current_status in ["paused", "cancelled"]:
                logger.info(f"🛑 Tarefa {task_id} foi {current_status}")
                return
            
            try:
                logger.info(f"📦 Processando variantes do produto {product_id} ({i+1}/{len(product_ids)})")
                
                # URL da API
                product_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
                headers = {
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
                
                # Buscar produto atual
                async with httpx.AsyncClient(timeout=30.0) as client:
                    get_response = await client.get(product_url, headers=headers)
                    
                    if get_response.status_code != 200:
                        raise Exception(f"Erro ao buscar produto: {get_response.status_code}")
                    
                    product_data = get_response.json()
                    current_product = product_data.get("product", {})
                    
                    # 🔴 MUDANÇA: PEGAR O TÍTULO DO PRODUTO
                    product_title = current_product.get("title", f"Produto {product_id}")
                    
                    # 🔴 MUDANÇA: ATUALIZAR PROGRESSO COM TÍTULO
                    if task_id in tasks_db:
                        tasks_db[task_id]["progress"]["current_product"] = product_title  # USANDO TÍTULO
                        tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                    
                    # Preparar payload de atualização baseado no submitData
                    update_payload = {
                        "product": {
                            "id": int(product_id)
                        }
                    }
                    
                    # Aplicar mudanças de título de opções
                    if submit_data.get("titleChanges"):
                        options = []
                        for i, option in enumerate(current_product.get("options", [])):
                            new_name = submit_data["titleChanges"].get(option["name"], option["name"])
                            options.append({
                                "id": option.get("id"),
                                "name": new_name,
                                "position": option.get("position", i + 1),
                                "values": option.get("values", [])
                            })
                        update_payload["product"]["options"] = options
                    
                    # Aplicar mudanças de variantes
                    if submit_data.get("valueChanges") or submit_data.get("newValues"):
                        variants = []
                        
                        for variant in current_product.get("variants", []):
                            updated_variant = {
                                "id": variant.get("id"),
                                "price": variant.get("price"),
                                "compare_at_price": variant.get("compare_at_price"),
                                "sku": variant.get("sku"),
                                "inventory_quantity": variant.get("inventory_quantity"),
                                "option1": variant.get("option1"),
                                "option2": variant.get("option2"),
                                "option3": variant.get("option3")
                            }
                            
                            # Aplicar mudanças de valores
                            for option_field in ["option1", "option2", "option3"]:
                                if variant.get(option_field):
                                    for option_name, changes in submit_data.get("valueChanges", {}).items():
                                        if variant[option_field] in changes:
                                            change = changes[variant[option_field]]
                                            updated_variant[option_field] = change.get("newName", variant[option_field])
                                            
                                            # Ajustar preço se houver mudança
                                            if "extraPrice" in change:
                                                current_price = float(variant.get("price", 0))
                                                extra_price = float(change["extraPrice"])
                                                updated_variant["price"] = str(current_price + extra_price)
                            
                            variants.append(updated_variant)
                        
                        # Adicionar novas variantes se houver
                        if submit_data.get("newValues"):
                            # Lógica para criar novas variantes baseada nos novos valores
                            # Isso seria mais complexo e dependeria da estrutura exata
                            pass
                        
                        update_payload["product"]["variants"] = variants
                    
                    # Enviar atualização
                    update_response = await client.put(
                        product_url,
                        headers=headers,
                        json=update_payload
                    )
                    
                    if update_response.status_code == 200:
                        successful += 1
                        result = {
                            "product_id": product_id,
                            "product_title": product_title,  # 🔴 MUDANÇA: INCLUIR TÍTULO NO RESULTADO
                            "status": "success",
                            "message": "Variantes atualizadas com sucesso"
                        }
                        logger.info(f"✅ Produto '{product_title}' atualizado")  # 🔴 MUDANÇA: LOG COM TÍTULO
                    else:
                        failed += 1
                        error_text = await update_response.text()
                        result = {
                            "product_id": product_id,
                            "product_title": product_title,  # 🔴 MUDANÇA: INCLUIR TÍTULO NO RESULTADO
                            "status": "failed",
                            "message": f"Erro: {error_text}"
                        }
                        logger.error(f"❌ Erro no produto '{product_title}'")  # 🔴 MUDANÇA: LOG COM TÍTULO
                
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
                    "current_product": None if i == len(product_ids)-1 else None  # 🔴 MUDANÇA: Limpar ao final
                }
                tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                tasks_db[task_id]["results"] = results[-50:]
            
            # Verificar novamente se foi pausado/cancelado
            if task_id in tasks_db:
                if tasks_db[task_id].get("status") in ["paused", "cancelled"]:
                    logger.info(f"🛑 Parando após processar {product_id}")
                    return
            
            # Rate limiting
            await asyncio.sleep(0.5)
    
    except Exception as e:
        logger.error(f"❌ Erro geral no processamento de variantes: {str(e)}")
    
    # Finalizar
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = get_brazil_time_str()
        tasks_db[task_id]["results"] = results
        tasks_db[task_id]["progress"]["current_product"] = None
        
        logger.info(f"🏁 PROCESSAMENTO DE VARIANTES FINALIZADO: ✅ {successful} | ❌ {failed}")

# Função auxiliar para processar variantes de um único produto
async def process_single_product_variants(
    task_id: str,
    product_id: str,
    submit_data: Dict,
    store_name: str,
    access_token: str
):
    """Processar variantes de um único produto"""
    
    logger.info(f"🚀 PROCESSANDO VARIANTES DO PRODUTO: {product_id}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    try:
        # URL da API
        product_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Buscar produto atual
            get_response = await client.get(product_url, headers=headers)
            
            if get_response.status_code != 200:
                raise Exception(f"Erro ao buscar produto: {get_response.status_code}")
            
            product_data = get_response.json()
            current_product = product_data.get("product", {})
            
            # 🔴 MUDANÇA: PEGAR O TÍTULO DO PRODUTO
            product_title = current_product.get("title", f"Produto {product_id}")
            
            # 🔴 MUDANÇA: ATUALIZAR STATUS DA TAREFA COM TÍTULO
            if task_id in tasks_db:
                tasks_db[task_id]["progress"]["current_product"] = product_title  # USANDO TÍTULO
                tasks_db[task_id]["updated_at"] = get_brazil_time_str()
            
            # Preparar payload de atualização
            update_payload = {
                "product": {
                    "id": int(product_id),
                    "options": [],
                    "variants": []
                }
            }
            
            # Aplicar mudanças de título de opções
            if submit_data.get("titleChanges"):
                for i, option in enumerate(current_product.get("options", [])):
                    new_name = submit_data["titleChanges"].get(option["name"], option["name"])
                    update_payload["product"]["options"].append({
                        "id": option.get("id"),
                        "name": new_name,
                        "position": option.get("position", i + 1),
                        "values": option.get("values", [])
                    })
            else:
                update_payload["product"]["options"] = current_product.get("options", [])
            
            # Aplicar mudanças nas variantes
            for variant in current_product.get("variants", []):
                updated_variant = {
                    "id": variant.get("id"),
                    "price": variant.get("price"),
                    "compare_at_price": variant.get("compare_at_price"),
                    "sku": variant.get("sku"),
                    "inventory_quantity": variant.get("inventory_quantity"),
                    "option1": variant.get("option1"),
                    "option2": variant.get("option2"),
                    "option3": variant.get("option3")
                }
                
                # Aplicar mudanças de valores e preços
                if submit_data.get("valueChanges"):
                    for option_name, changes in submit_data["valueChanges"].items():
                        for option_field in ["option1", "option2", "option3"]:
                            if variant.get(option_field) in changes:
                                change = changes[variant[option_field]]
                                updated_variant[option_field] = change.get("newName", variant[option_field])
                                
                                # Ajustar preço se houver mudança
                                if "extraPrice" in change:
                                    current_price = float(variant.get("price", 0))
                                    original_extra = float(change.get("originalExtraPrice", 0))
                                    new_extra = float(change["extraPrice"])
                                    base_price = current_price - original_extra
                                    updated_variant["price"] = str(base_price + new_extra)
                                    
                                    # Atualizar compare_at_price se existir
                                    if variant.get("compare_at_price"):
                                        compare_price = float(variant["compare_at_price"])
                                        base_compare = compare_price - original_extra
                                        updated_variant["compare_at_price"] = str(base_compare + new_extra)
                
                update_payload["product"]["variants"].append(updated_variant)
            
            # Adicionar novas variantes se houver
            if submit_data.get("newValues"):
                # Esta parte seria mais complexa e dependeria de como as novas variantes são estruturadas
                logger.info(f"📝 Novas variantes a serem criadas: {submit_data['newValues']}")
            
            # Enviar atualização
            update_response = await client.put(
                product_url,
                headers=headers,
                json=update_payload
            )
            
            if update_response.status_code == 200:
                if task_id in tasks_db:
                    tasks_db[task_id]["status"] = "completed"
                    tasks_db[task_id]["completed_at"] = get_brazil_time_str()
                    tasks_db[task_id]["progress"]["processed"] = 1
                    tasks_db[task_id]["progress"]["successful"] = 1
                    tasks_db[task_id]["progress"]["percentage"] = 100
                logger.info(f"✅ Produto '{product_title}' atualizado com sucesso")  # 🔴 MUDANÇA: LOG COM TÍTULO
            else:
                error_text = await update_response.text()
                if task_id in tasks_db:
                    tasks_db[task_id]["status"] = "failed"
                    tasks_db[task_id]["error_message"] = error_text
                    tasks_db[task_id]["completed_at"] = get_brazil_time_str()
                    tasks_db[task_id]["progress"]["processed"] = 1
                    tasks_db[task_id]["progress"]["failed"] = 1
                logger.error(f"❌ Erro ao atualizar produto '{product_title}': {error_text}")  # 🔴 MUDANÇA: LOG COM TÍTULO
    
    except Exception as e:
        logger.error(f"❌ Exceção no processamento de variantes: {str(e)}")
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error_message"] = str(e)
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()
            tasks_db[task_id]["progress"]["processed"] = 1
            tasks_db[task_id]["progress"]["failed"] = 1

# ==================== AGENDAMENTO DE TAREFAS ====================

@app.post("/api/tasks/schedule")
async def schedule_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Criar nova tarefa agendada"""
    task_id = data.get("id") or f"task_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    # LOG PARA DEBUG
    logger.info(f"📋 Recebendo agendamento: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    scheduled_for = data.get("scheduled_for", get_brazil_time_str())
    
    # CORREÇÃO DE TIMEZONE - Assumir que o horário vem em UTC se tiver 'Z'
    if scheduled_for.endswith('Z'):
        # Remove o 'Z' e adiciona timezone UTC
        scheduled_for_clean = scheduled_for[:-1]
        scheduled_time = datetime.fromisoformat(scheduled_for_clean).replace(tzinfo=timezone.utc)
        # Converter para horário local do servidor
        scheduled_time_local = scheduled_time.astimezone()
        # Remover timezone para comparação
        scheduled_time_naive = scheduled_time_local.replace(tzinfo=None)
    else:
        # Se não tem 'Z', assumir que é horário local
        try:
            scheduled_time = datetime.fromisoformat(scheduled_for)
            if scheduled_time.tzinfo is not None:
                scheduled_time_naive = scheduled_time.replace(tzinfo=None)
            else:
                scheduled_time_naive = scheduled_time
        except:
            scheduled_time_naive = datetime.fromisoformat(scheduled_for.replace('Z', ''))
    
    now = datetime.now()
    
    # LOG do horário convertido
    logger.info(f"📅 Horário original: {scheduled_for}")
    logger.info(f"📅 Horário convertido para local: {scheduled_time_naive}")
    logger.info(f"📅 Horário atual do servidor: {now}")
    
    # Se já passou, executar imediatamente
    if scheduled_time_naive <= now:
        logger.info(f"📅 Tarefa {task_id} agendada para horário passado, executando imediatamente!")
        
        task = {
            "id": task_id,
            "name": data.get("name", "Tarefa Agendada"),
            "task_type": data.get("task_type", "bulk_edit"),
            "status": "processing",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),  # Adicionar horário local
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
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
            "scheduled_for_local": scheduled_time_naive.isoformat(),  # Adicionar horário local
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        logger.info(f"📅 Tarefa {task_id} agendada para {scheduled_time_naive} (horário local)")
        
        # LOG ADICIONAL
        diff = (scheduled_time_naive - now).total_seconds()
        logger.info(f"⏱️ Tarefa será executada em {diff:.0f} segundos ({diff/60:.1f} minutos)")
    
    return {
        "success": True,
        "taskId": task_id,
        "task": task
    }

# ==================== AGENDAMENTO DE VARIANTES ====================

@app.post("/api/tasks/schedule-variants")
async def schedule_variants_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Agendar tarefa de variantes - endpoint específico"""
    
    task_id = data.get("id") or f"scheduled_variant_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    # LOG PARA DEBUG
    logger.info(f"📋 Recebendo agendamento de variantes: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    scheduled_for = data.get("scheduled_for", get_brazil_time_str())
    
    # CORREÇÃO DE TIMEZONE - Assumir que o horário vem em UTC se tiver 'Z'
    if scheduled_for.endswith('Z'):
        scheduled_for_clean = scheduled_for[:-1]
        scheduled_time = datetime.fromisoformat(scheduled_for_clean).replace(tzinfo=timezone.utc)
        scheduled_time_local = scheduled_time.astimezone()
        scheduled_time_naive = scheduled_time_local.replace(tzinfo=None)
    else:
        try:
            scheduled_time = datetime.fromisoformat(scheduled_for)
            if scheduled_time.tzinfo is not None:
                scheduled_time_naive = scheduled_time.replace(tzinfo=None)
            else:
                scheduled_time_naive = scheduled_time
        except:
            scheduled_time_naive = datetime.fromisoformat(scheduled_for.replace('Z', ''))
    
    now = datetime.now()
    
    # LOG do horário convertido
    logger.info(f"📅 Horário original: {scheduled_for}")
    logger.info(f"📅 Horário convertido para local: {scheduled_time_naive}")
    logger.info(f"📅 Horário atual do servidor: {now}")
    
    # Se já passou, executar imediatamente
    if scheduled_time_naive <= now:
        logger.info(f"📅 Tarefa de variantes {task_id} agendada para horário passado, executando imediatamente!")
        
        task = {
            "id": task_id,
            "name": data.get("name", "Gerenciamento de Variantes"),
            "task_type": "variant_management",
            "status": "processing",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
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
        
        # Verificar se tem CSV ou submitData para processamento de variantes
        if config.get("csvContent"):
            # Processar com CSV
            background_tasks.add_task(
                process_variants_background,
                task_id,
                config.get("csvContent", ""),
                config.get("productIds", []),
                config.get("submitData", {}),
                config.get("storeName", ""),
                config.get("accessToken", "")
            )
        elif config.get("submitData") and config.get("productId"):
            # Processar diretamente com submitData (para um produto)
            background_tasks.add_task(
                process_single_product_variants,
                task_id,
                config.get("productId"),
                config.get("submitData", {}),
                config.get("storeName", ""),
                config.get("accessToken", "")
            )
        else:
            logger.error(f"❌ Configuração inválida para tarefa de variantes {task_id}")
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error_message"] = "Configuração inválida: faltam dados necessários"
            return {
                "success": False,
                "message": "Configuração inválida para tarefa de variantes"
            }
        
        logger.info(f"▶️ Tarefa de variantes {task_id} iniciada imediatamente")
    else:
        # Agendar normalmente
        task = {
            "id": task_id,
            "name": data.get("name", "Gerenciamento de Variantes"),
            "task_type": "variant_management",
            "status": "scheduled",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": data.get("config", {}),
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        logger.info(f"📅 Tarefa de variantes {task_id} agendada para {scheduled_time_naive} (horário local)")
        
        # LOG ADICIONAL
        diff = (scheduled_time_naive - now).total_seconds()
        logger.info(f"⏱️ Tarefa de variantes será executada em {diff:.0f} segundos ({diff/60:.1f} minutos)")
    
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
    task["started_at"] = get_brazil_time_str()
    task["updated_at"] = get_brazil_time_str()
    
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
    task["paused_at"] = get_brazil_time_str()
    task["updated_at"] = get_brazil_time_str()
    
    logger.info(f"⏸️ Tarefa {task_id} pausada")
    
    return {
        "success": True,
        "message": "Tarefa pausada com sucesso",
        "task": task
    }

@app.post("/api/tasks/resume/{task_id}")
async def resume_task(task_id: str, background_tasks: BackgroundTasks):
    """Retomar uma tarefa pausada - VERSÃO MELHORADA"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    if task["status"] != "paused":
        logger.warning(f"⚠️ Tentativa de retomar tarefa não pausada: {task_id} (status: {task['status']})")
        return {
            "success": False,
            "message": f"Tarefa não está pausada (status atual: {task['status']})"
        }
    
    # Mudar status para processing
    task["status"] = "processing"
    task["resumed_at"] = get_brazil_time_str()
    task["updated_at"] = get_brazil_time_str()
    
    # Continuar de onde parou
    config = task.get("config", {})
    all_product_ids = config.get("productIds", [])
    
    # CORREÇÃO: Garantir que temos o progresso correto
    processed_count = task.get("progress", {}).get("processed", 0)
    remaining_products = all_product_ids[processed_count:]
    
    logger.info(f"▶️ Retomando tarefa {task_id}")
    logger.info(f"   Total de produtos: {len(all_product_ids)}")
    logger.info(f"   Já processados: {processed_count}")
    logger.info(f"   Restantes: {len(remaining_products)}")
    
    if len(remaining_products) > 0:
        # MANTER background_tasks.add_task (MAIS SEGURO!)
        background_tasks.add_task(
            process_products_background,
            task_id,
            remaining_products,
            config.get("operations", []),
            config.get("storeName", ""),
            config.get("accessToken", ""),
            is_resume=True
        )
        
        logger.info(f"✅ Tarefa {task_id} retomada com {len(remaining_products)} produtos")
        
        return {
            "success": True,
            "message": f"Tarefa retomada com sucesso",
            "task": task,
            "remaining": len(remaining_products)
        }
    else:
        # Se não há produtos restantes, marcar como completa
        task["status"] = "completed"
        task["completed_at"] = get_brazil_time_str()
        
        return {
            "success": True,
            "message": "Tarefa já estava completa",
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
    task["cancelled_at"] = get_brazil_time_str()
    task["updated_at"] = get_brazil_time_str()
    
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
    
    task["updated_at"] = get_brazil_time_str()
    
    # IMPORTANTE: Se atualizou o scheduled_for e já passou, executar IMEDIATAMENTE
    if "scheduled_for" in data and task["status"] == "scheduled":
        scheduled_for = data["scheduled_for"]
        
        # CORREÇÃO DE TIMEZONE
        if scheduled_for.endswith('Z'):
            scheduled_for_clean = scheduled_for[:-1]
            scheduled_time = datetime.fromisoformat(scheduled_for_clean).replace(tzinfo=timezone.utc)
            scheduled_time = scheduled_time.astimezone().replace(tzinfo=None)
        else:
            try:
                scheduled_time = datetime.fromisoformat(scheduled_for)
                if scheduled_time.tzinfo is not None:
                    scheduled_time = scheduled_time.replace(tzinfo=None)
            except:
                scheduled_time = datetime.fromisoformat(scheduled_for.replace('Z', ''))
        
        # Também atualizar o scheduled_for_local
        task["scheduled_for_local"] = scheduled_time.isoformat()
        
        now = datetime.now()
        
        if scheduled_time <= now:
            logger.info(f"📝 Tarefa {task_id} atualizada para horário passado, executando imediatamente!")
            
            # Mudar status e processar
            task["status"] = "processing"
            task["started_at"] = get_brazil_time_str()
            
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
        "timestamp": get_brazil_time_str()
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
    """PROCESSAR PRODUTOS EM BACKGROUND - VERSÃO MELHORADA"""
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
            # VERIFICAR STATUS ANTES DE PROCESSAR CADA PRODUTO
            if task_id not in tasks_db:
                logger.warning(f"⚠️ Tarefa {task_id} não existe mais")
                return
                
            current_status = tasks_db[task_id].get("status")
            
            # PARAR IMEDIATAMENTE SE PAUSADO OU CANCELADO
            if current_status in ["paused", "cancelled"]:
                logger.info(f"🛑 Tarefa {task_id} foi {current_status}, parando processamento IMEDIATAMENTE")
                # Salvar progresso atual antes de parar
                if current_status == "paused" and task_id in tasks_db:
                    tasks_db[task_id]["progress"]["current_product"] = None
                return
            
            try:
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
                
                # 🔴 MUDANÇA: PEGAR O TÍTULO DO PRODUTO
                product_title = current_product.get("title", "Sem título")
                
                # 🔴 MUDANÇA: ATUALIZAR PROGRESSO COM TÍTULO ANTES DE PROCESSAR
                if task_id in tasks_db:
                    tasks_db[task_id]["progress"]["current_product"] = product_title  # USANDO TÍTULO
                    tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                
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
                        "product_title": product_title,  # 🔴 MUDANÇA: INCLUIR TÍTULO
                        "status": "success",
                        "message": "Produto atualizado com sucesso"
                    }
                    logger.info(f"✅ Produto '{product_title}' atualizado")  # 🔴 MUDANÇA: LOG COM TÍTULO
                else:
                    failed += 1
                    error_text = await update_response.text()
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,  # 🔴 MUDANÇA: INCLUIR TÍTULO
                        "status": "failed",
                        "message": f"Erro HTTP {update_response.status_code}: {error_text}"
                    }
                    logger.error(f"❌ Erro no produto '{product_title}': {error_text}")  # 🔴 MUDANÇA: LOG COM TÍTULO
                    
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
                    "current_product": None if i == len(product_ids)-1 else None  # 🔴 MUDANÇA: Limpar ao final
                }
                tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                tasks_db[task_id]["results"] = results[-50:]
            
            # VERIFICAR NOVAMENTE APÓS PROCESSAR CADA PRODUTO
            if task_id in tasks_db:
                if tasks_db[task_id].get("status") in ["paused", "cancelled"]:
                    logger.info(f"🛑 Parando após processar {product_id}")
                    return
            
            # Rate limiting
            await asyncio.sleep(0.3)
    
    # Finalizar
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = get_brazil_time_str()
        tasks_db[task_id]["results"] = results
        tasks_db[task_id]["progress"]["current_product"] = None
        
        logger.info(f"🏁 TAREFA FINALIZADA: ✅ {successful} | ❌ {failed}")

# ==================== VERIFICADOR DE TAREFAS AGENDADAS ====================

async def check_and_execute_scheduled_tasks():
    """Verificar e executar tarefas agendadas automaticamente"""
    while True:
        try:
            now = datetime.now()
            
            for task_id, task in list(tasks_db.items()):
                if task["status"] == "scheduled":
                    # Usar scheduled_for_local se disponível, senão usar scheduled_for
                    scheduled_for = task.get("scheduled_for_local") or task["scheduled_for"]
                    
                    # Processar o horário
                    if scheduled_for.endswith('Z'):
                        scheduled_for_clean = scheduled_for[:-1]
                        scheduled_time = datetime.fromisoformat(scheduled_for_clean).replace(tzinfo=timezone.utc)
                        scheduled_time = scheduled_time.astimezone().replace(tzinfo=None)
                    else:
                        try:
                            scheduled_time = datetime.fromisoformat(scheduled_for)
                            if scheduled_time.tzinfo is not None:
                                scheduled_time = scheduled_time.replace(tzinfo=None)
                        except:
                            scheduled_time = datetime.fromisoformat(scheduled_for.replace('Z', ''))
                    
                    # Se já passou do horário, executar
                    if scheduled_time <= now:
                        logger.info(f"⏰ Executando tarefa agendada {task_id}")
                        logger.info(f"   Agendada para: {scheduled_time}")
                        logger.info(f"   Horário atual: {now}")
                        
                        # Mudar status e processar
                        task["status"] = "processing"
                        task["started_at"] = get_brazil_time_str()
                        task["updated_at"] = get_brazil_time_str()
                        
                        config = task.get("config", {})
                        
                        # Verificar o tipo de tarefa
                        if task.get("task_type") == "variant_management":
                            # Processar variantes
                            if config.get("csvContent"):
                                asyncio.create_task(
                                    process_variants_background(
                                        task_id,
                                        config.get("csvContent", ""),
                                        config.get("productIds", []),
                                        config.get("submitData", {}),
                                        config.get("storeName", ""),
                                        config.get("accessToken", "")
                                    )
                                )
                            elif config.get("submitData") and config.get("productId"):
                                asyncio.create_task(
                                    process_single_product_variants(
                                        task_id,
                                        config.get("productId"),
                                        config.get("submitData", {}),
                                        config.get("storeName", ""),
                                        config.get("accessToken", "")
                                    )
                                )
                        else:
                            # Processar edição em massa normal
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