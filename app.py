from fastapi import FastAPI, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect, File, UploadFile, Form
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Set
import httpx
import asyncio
import json
import secrets
from datetime import datetime, timezone, timedelta
import pytz
import uvicorn
import logging
import re
import csv
import io
import base64
import os

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

# Gerenciador de WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

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

# ==================== ENDPOINTS DE ALT-TEXT E IMAGENS (CSV) ====================
@app.post("/api/images/import-csv")
async def import_images_csv(data: Dict[str, Any]):
    """Importa alt-text de um arquivo CSV"""
    try:
        csv_data = data.get('csvData')
        store_name = data.get('storeName')
        access_token = data.get('accessToken')
        dry_run = data.get('dryRun', False)
        
        if not csv_data or not store_name or not access_token:
            return {
                'success': False,
                'message': 'Dados ou conexão não fornecidos'
            }
        
        results = []
        successful = 0
        failed = 0
        unchanged = 0
        
        clean_store = store_name.replace('.myshopify.com', '')
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for image_data in csv_data:
                try:
                    # Renderizar template com dados completos
                    final_alt_text = image_data.get('template_used', '')
                    
                    # Substituir variáveis do produto
                    replacements = {
                        r'\{\{\s*product\.title\s*\}\}': image_data.get('product_title', ''),
                        r'\{\{\s*product\.handle\s*\}\}': image_data.get('product_handle', ''),
                        r'\{\{\s*product\.vendor\s*\}\}': image_data.get('product_vendor', ''),
                        r'\{\{\s*product\.type\s*\}\}': image_data.get('product_type', ''),
                        r'\{\{\s*image\.position\s*\}\}': str(image_data.get('image_position', '1')),
                        r'\{\{\s*variant\.name1\s*\}\}': image_data.get('variant_name1', ''),
                        r'\{\{\s*variant\.name2\s*\}\}': image_data.get('variant_name2', ''),
                        r'\{\{\s*variant\.name3\s*\}\}': image_data.get('variant_name3', ''),
                        r'\{\{\s*variant\.value1\s*\}\}': image_data.get('variant_value1', ''),
                        r'\{\{\s*variant\.value2\s*\}\}': image_data.get('variant_value2', ''),
                        r'\{\{\s*variant\.value3\s*\}\}': image_data.get('variant_value3', ''),
                    }
                    
                    for pattern, replacement in replacements.items():
                        final_alt_text = re.sub(pattern, replacement, final_alt_text)
                    
                    # Limpar texto final
                    final_alt_text = ' '.join(final_alt_text.split()).strip()
                    
                    # Verificar se precisa de atualização
                    if image_data.get('current_alt_text') == final_alt_text:
                        logger.info(f"ℹ️ Alt-text já correto para imagem {image_data.get('image_id')}")
                        unchanged += 1
                        continue
                    
                    if dry_run:
                        logger.info(f"🧪 DRY RUN: Atualizaria imagem {image_data.get('image_id')} com: '{final_alt_text}'")
                        successful += 1
                        continue
                    
                    # Atualizar via API Shopify
                    shopify_url = f"https://{clean_store}.myshopify.com/admin/api/2024-01/products/{image_data.get('product_id')}/images/{image_data.get('image_id')}.json"
                    
                    headers = {
                        'X-Shopify-Access-Token': access_token,
                        'Content-Type': 'application/json'
                    }
                    
                    update_data = {
                        'image': {
                            'id': int(image_data.get('image_id')),
                            'alt': final_alt_text
                        }
                    }
                    
                    response = await client.put(shopify_url, json=update_data, headers=headers)
                    
                    if response.status_code == 200:
                        logger.info(f"✅ Alt-text atualizado: imagem {image_data.get('image_id')} → '{final_alt_text}'")
                        successful += 1
                        results.append({
                            'image_id': image_data.get('image_id'),
                            'product_id': image_data.get('product_id'),
                            'status': 'success',
                            'old_alt': image_data.get('current_alt_text'),
                            'new_alt': final_alt_text
                        })
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Erro Shopify para imagem {image_data.get('image_id')}: {error_text}")
                        failed += 1
                        results.append({
                            'image_id': image_data.get('image_id'),
                            'status': 'failed',
                            'error': f"HTTP {response.status_code}: {error_text}"
                        })
                        
                except Exception as e:
                    logger.error(f"❌ Erro ao processar imagem {image_data.get('image_id')}: {str(e)}")
                    failed += 1
                    results.append({
                        'image_id': image_data.get('image_id'),
                        'status': 'failed',
                        'error': str(e)
                    })
                
                # Pausa entre requests
                await asyncio.sleep(0.2)
        
        stats = {
            'total': len(csv_data),
            'successful': successful,
            'failed': failed,
            'unchanged': unchanged,
            'processed': successful + failed + unchanged
        }
        
        logger.info(f"🏁 Processamento concluído: {stats}")
        
        return {
            'success': True,
            'message': f"Processamento concluído: {successful} sucessos, {failed} falhas, {unchanged} inalterados",
            'stats': stats,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"❌ Erro no processamento: {str(e)}")
        return {
            'success': False,
            'message': f"Erro no processamento: {str(e)}"
        }

@app.post("/api/images/export-csv")
async def export_images_csv(data: Dict[str, Any]):
    """Exporta imagens para CSV"""
    try:
        from fastapi.responses import StreamingResponse
        
        images = data.get('images', [])
        
        # Criar CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Headers
        headers = [
            'image_id', 'product_id', 'product_title', 'product_handle',
            'product_vendor', 'product_type', 'image_position', 
            'current_alt_text', 'new_alt_text', 'template_used',
            'variant_name1', 'variant_value1',
            'variant_name2', 'variant_value2',
            'variant_name3', 'variant_value3'
        ]
        writer.writerow(headers)
        
        # Dados
        for img in images:
            row = [
                img.get('id'),
                img.get('product_id'),
                img.get('product_title'),
                img.get('product_handle'),
                img.get('product_vendor'),
                img.get('product_type'),
                img.get('position'),
                img.get('alt', ''),
                '',  # new_alt_text - vazio para o usuário preencher
                '',  # template_used - vazio para o usuário preencher
                img.get('variant_name1', ''),
                img.get('variant_value1', ''),
                img.get('variant_name2', ''),
                img.get('variant_value2', ''),
                img.get('variant_name3', ''),
                img.get('variant_value3', '')
            ]
            writer.writerow(row)
        
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename=images-alt-text-{datetime.now().strftime("%Y%m%d")}.csv'
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Erro ao exportar CSV: {str(e)}")
        return {
            'success': False,
            'message': f"Erro ao exportar: {str(e)}"
        }

# ==================== ENDPOINTS DE ALT-TEXT COM BACKGROUND E AGENDAMENTO ====================
@app.post("/process-alt-text")
async def process_alt_text_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Processar alt-text em background"""
    
    task_id = data.get("id") or f"alt_text_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    logger.info(f"📋 Nova tarefa de alt-text {task_id}")
    
    csv_data = data.get("csvData", [])
    store_name = data.get("storeName", "")
    access_token = data.get("accessToken", "")
    
    if not csv_data or not store_name or not access_token:
        raise HTTPException(status_code=400, detail="Dados incompletos para processamento")
    
    # Salvar tarefa na memória
    tasks_db[task_id] = {
        "id": task_id,
        "name": f"Alt-Text SEO - {len(csv_data)} imagens",
        "status": "processing",
        "task_type": "alt_text",
        "progress": {
            "processed": 0,
            "total": len(csv_data),
            "successful": 0,
            "failed": 0,
            "unchanged": 0,
            "percentage": 0,
            "current_image": None
        },
        "started_at": get_brazil_time_str(),
        "updated_at": get_brazil_time_str(),
        "config": {
            "csvData": csv_data,
            "storeName": store_name,
            "accessToken": access_token,
            "itemCount": len(csv_data)
        },
        "results": []
    }
    
    logger.info(f"✅ Tarefa de alt-text {task_id} iniciada")
    
    # Processar em background
    background_tasks.add_task(
        process_alt_text_background,
        task_id,
        csv_data,
        store_name,
        access_token
    )
    
    return {
        "success": True,
        "message": f"Processamento de alt-text iniciado para {len(csv_data)} imagens",
        "taskId": task_id,
        "estimatedTime": f"{len(csv_data) * 0.2:.1f} segundos",
        "mode": "background_processing"
    }

@app.post("/api/tasks/schedule-alt-text")
async def schedule_alt_text_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Agendar tarefa de alt-text"""
    
    task_id = data.get("id") or f"scheduled_alt_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    logger.info(f"📋 Recebendo agendamento de alt-text: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    scheduled_for = data.get("scheduled_for", get_brazil_time_str())
    
    # Processar timezone
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
    
    logger.info(f"📅 Horário convertido para local: {scheduled_time_naive}")
    logger.info(f"📅 Horário atual do servidor: {now}")
    
    # NOVO: Processar notificações se configuradas
    notification_scheduled_for = None
    if data.get("notifications"):
        notifications = data["notifications"]
        if notifications.get("before_execution"):
            notification_time_minutes = notifications.get("notification_time", 30)
            
            # Calcular horário da notificação
            notification_datetime = scheduled_time_naive - timedelta(minutes=notification_time_minutes)
            notification_scheduled_for = notification_datetime.isoformat()
            
            logger.info(f"📱 Notificação configurada para: {notification_datetime}")
            logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
    
    # Se já passou, executar imediatamente
    if scheduled_time_naive <= now:
        logger.info(f"📅 Tarefa de alt-text {task_id} agendada para horário passado, executando imediatamente!")
        
        task = {
            "id": task_id,
            "name": data.get("name", "Alt-Text SEO"),
            "task_type": "alt_text",
            "status": "processing",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,  # NOVO
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")  # NOVO: Salvar notificações
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "unchanged": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        
        # Processar imediatamente
        config = task.get("config", {})
        background_tasks.add_task(
            process_alt_text_background,
            task_id,
            config.get("csvData", []),
            config.get("storeName", ""),
            config.get("accessToken", "")
        )
        
        logger.info(f"▶️ Tarefa de alt-text {task_id} iniciada imediatamente")
    else:
        # Agendar normalmente
        task = {
            "id": task_id,
            "name": data.get("name", "Alt-Text SEO"),
            "task_type": "alt_text",
            "status": "scheduled",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,  # NOVO
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")  # NOVO: Salvar notificações
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "unchanged": 0,
                "percentage": 0
            }
        }
        
        tasks_db[task_id] = task
        logger.info(f"📅 Tarefa de alt-text {task_id} agendada para {scheduled_time_naive}")
        
        diff = (scheduled_time_naive - now).total_seconds()
        logger.info(f"⏱️ Tarefa será executada em {diff:.0f} segundos ({diff/60:.1f} minutos)")
    
    return {
        "success": True,
        "taskId": task_id,
        "task": task
    }

async def process_alt_text_background(
    task_id: str,
    csv_data: List[Dict],
    store_name: str,
    access_token: str,
    is_resume: bool = False
):
    """Processar alt-text em background"""
    
    if not is_resume:
        logger.info(f"🚀 INICIANDO PROCESSAMENTO DE ALT-TEXT: {task_id}")
    else:
        logger.info(f"▶️ RETOMANDO PROCESSAMENTO DE ALT-TEXT: {task_id}")
    
    logger.info(f"📸 Imagens para processar: {len(csv_data)}")
    
    clean_store = store_name.replace('.myshopify.com', '')
    
    # Se for retomada, pegar progresso existente
    if is_resume and task_id in tasks_db:
        task = tasks_db[task_id]
        processed = task["progress"]["processed"]
        successful = task["progress"]["successful"]
        failed = task["progress"]["failed"]
        unchanged = task["progress"].get("unchanged", 0)
        results = task.get("results", [])
        total = task["progress"]["total"]
    else:
        processed = 0
        successful = 0
        failed = 0
        unchanged = 0
        results = []
        total = len(csv_data)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, image_data in enumerate(csv_data[processed:], start=processed):
            # Verificar se a tarefa foi pausada ou cancelada
            if task_id not in tasks_db:
                logger.warning(f"⚠️ Tarefa {task_id} não existe mais")
                return
            
            current_status = tasks_db[task_id].get("status")
            
            if current_status in ["paused", "cancelled"]:
                logger.info(f"🛑 Tarefa {task_id} foi {current_status}")
                return
            
            try:
                # Renderizar template
                final_alt_text = image_data.get('template_used', '')
                
                # Substituir variáveis
                replacements = {
                    r'\{\{\s*product\.title\s*\}\}': image_data.get('product_title', ''),
                    r'\{\{\s*product\.handle\s*\}\}': image_data.get('product_handle', ''),
                    r'\{\{\s*product\.vendor\s*\}\}': image_data.get('product_vendor', ''),
                    r'\{\{\s*product\.type\s*\}\}': image_data.get('product_type', ''),
                    r'\{\{\s*image\.position\s*\}\}': str(image_data.get('image_position', '1')),
                    r'\{\{\s*variant\.name1\s*\}\}': image_data.get('variant_name1', ''),
                    r'\{\{\s*variant\.name2\s*\}\}': image_data.get('variant_name2', ''),
                    r'\{\{\s*variant\.name3\s*\}\}': image_data.get('variant_name3', ''),
                    r'\{\{\s*variant\.value1\s*\}\}': image_data.get('variant_value1', ''),
                    r'\{\{\s*variant\.value2\s*\}\}': image_data.get('variant_value2', ''),
                    r'\{\{\s*variant\.value3\s*\}\}': image_data.get('variant_value3', ''),
                }
                
                for pattern, replacement in replacements.items():
                    final_alt_text = re.sub(pattern, replacement, final_alt_text)
                
                final_alt_text = ' '.join(final_alt_text.split()).strip()
                
                # Verificar se precisa de atualização
                if image_data.get('current_alt_text') == final_alt_text:
                    logger.info(f"ℹ️ Alt-text já correto para imagem {image_data.get('image_id')}")
                    unchanged += 1
                    processed += 1
                    continue
                
                # Atualizar via API Shopify
                shopify_url = f"https://{clean_store}.myshopify.com/admin/api/2024-01/products/{image_data.get('product_id')}/images/{image_data.get('image_id')}.json"
                
                headers = {
                    'X-Shopify-Access-Token': access_token,
                    'Content-Type': 'application/json'
                }
                
                update_data = {
                    'image': {
                        'id': int(image_data.get('image_id')),
                        'alt': final_alt_text
                    }
                }
                
                response = await client.put(shopify_url, json=update_data, headers=headers)
                
                if response.status_code == 200:
                    logger.info(f"✅ Alt-text atualizado: imagem {image_data.get('image_id')}")
                    successful += 1
                    results.append({
                        'image_id': image_data.get('image_id'),
                        'product_id': image_data.get('product_id'),
                        'status': 'success',
                        'old_alt': image_data.get('current_alt_text'),
                        'new_alt': final_alt_text
                    })
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Erro Shopify: {error_text}")
                    failed += 1
                    results.append({
                        'image_id': image_data.get('image_id'),
                        'status': 'failed',
                        'error': f"HTTP {response.status_code}: {error_text}"
                    })
                    
            except Exception as e:
                logger.error(f"❌ Erro ao processar imagem: {str(e)}")
                failed += 1
                results.append({
                    'image_id': image_data.get('image_id'),
                    'status': 'failed',
                    'error': str(e)
                })
            
            # Atualizar progresso
            processed += 1
            percentage = round((processed / total) * 100)
            
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "unchanged": unchanged,
                    "percentage": percentage,
                    "current_image": f"Imagem {image_data.get('image_id')}" if i < len(csv_data)-1 else None
                }
                tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                tasks_db[task_id]["results"] = results[-50:]
            
            # Verificar novamente se foi pausado/cancelado
            if task_id in tasks_db:
                if tasks_db[task_id].get("status") in ["paused", "cancelled"]:
                    logger.info(f"🛑 Parando após processar imagem {image_data.get('image_id')}")
                    return
            
            # Rate limiting
            await asyncio.sleep(0.2)
    
    # Finalizar
    final_status = "completed" if failed == 0 else "completed_with_errors"
    
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = final_status
        tasks_db[task_id]["completed_at"] = get_brazil_time_str()
        tasks_db[task_id]["results"] = results
        tasks_db[task_id]["progress"]["current_image"] = None
        
        logger.info(f"🏁 ALT-TEXT FINALIZADO: ✅ {successful} | ❌ {failed} | ⚪ {unchanged}")

# ==================== ENDPOINT DE RENOMEAÇÃO DE IMAGENS - VERSÃO DIRETA SEM ATTACHMENT ====================

@app.post("/api/rename/process")
async def process_rename_images(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Endpoint para "renomear" imagens no Shopify
    Como não é possível renomear diretamente, fazemos: download -> reupload com novo nome -> delete antiga
    AGORA USANDO URLs DIRETAS DO FRONTEND!
    """
    
    try:
        logger.info(f"📋 Requisição de renomeação recebida")
        logger.info(f"   Template: {data.get('template', 'N/A')}")
        logger.info(f"   Imagens: {len(data.get('images', []))}")
        logger.info(f"   Loja: {data.get('storeName', 'N/A')}")
        
        task_id = data.get("id") or f"rename_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
        
        template = data.get("template", "")
        images = data.get("images", [])
        store_name = data.get("storeName", "")
        access_token = data.get("accessToken", "")
        
        # Validação
        if not template:
            raise HTTPException(status_code=400, detail="Template de renomeação não fornecido")
        if not images:
            raise HTTPException(status_code=400, detail="Nenhuma imagem fornecida para renomear")
        if not store_name:
            raise HTTPException(status_code=400, detail="Nome da loja não fornecido")
        if not access_token:
            raise HTTPException(status_code=400, detail="Token de acesso não fornecido")
        
        logger.info(f"✅ Validação concluída - {len(images)} imagens para processar")
        logger.info(f"✅ Criando tarefa {task_id}")
        
        # OTIMIZAÇÃO 1: NÃO ARMAZENAR ARRAY COMPLETO NO CONFIG
        # Salvar tarefa na memória - SEM O ARRAY DE IMAGENS
        tasks_db[task_id] = {
            "id": task_id,
            "name": f"Renomeação - {len(images)} imagens",
            "status": "processing",
            "task_type": "rename_images",
            "progress": {
                "processed": 0,
                "total": len(images),
                "successful": 0,
                "failed": 0,
                "unchanged": 0,
                "percentage": 0,
                "current_image": None
            },
            "started_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "config": {
                "template": template,
                # REMOVIDO: "images": images,  # NÃO ARMAZENAR ARRAY COMPLETO!
                "storeName": store_name,
                "accessToken": access_token,
                "itemCount": len(images)
            },
            "results": []  # Será limitado durante o processo
        }
        
        logger.info(f"✅ Tarefa {task_id} criada com {len(images)} imagens")
        
        # Processar em background
        background_tasks.add_task(
            process_rename_images_background,
            task_id,
            template,
            images,
            store_name,
            access_token
        )
        
        return {
            "success": True,
            "message": f"Processamento de renomeação iniciado para {len(images)} imagens",
            "taskId": task_id,
            "estimatedTime": f"{len(images) * 1.5:.1f} segundos",
            "mode": "background_processing"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

async def process_rename_images_background(
    task_id: str,
    template: str,
    images: List[Dict],
    store_name: str,
    access_token: str,
    is_resume: bool = False
):
    """
    Processa "renomeação" de imagens: Download -> Processar com Pillow -> Reupload -> Delete
    AGORA USANDO URLs DIRETAS DO FRONTEND!
    """
    
    try:
        # Importar Pillow
        from PIL import Image
        import io
        import base64
        
        if not is_resume:
            logger.info(f"🚀 INICIANDO PROCESSO DE RENOMEAÇÃO: {task_id}")
            logger.info(f"🎨 Usando URLs diretas do frontend + Pillow para preservar transparência")
        else:
            logger.info(f"▶️ RETOMANDO RENOMEAÇÃO: {task_id}")
        
        logger.info(f"📸 Template: {template}")
        logger.info(f"📸 Total de imagens: {len(images)}")
        
        # Limpar nome da loja
        clean_store = store_name.replace('.myshopify.com', '').strip()
        api_version = '2024-01'
        
        # Se for retomada, pegar progresso existente
        if is_resume and task_id in tasks_db:
            task = tasks_db[task_id]
            processed = task["progress"]["processed"]
            successful = task["progress"]["successful"]
            failed = task["progress"]["failed"]
            unchanged = task["progress"].get("unchanged", 0)
            results = task.get("results", [])
            total = task["progress"]["total"]
        else:
            processed = 0
            successful = 0
            failed = 0
            unchanged = 0
            results = []
            total = len(images)
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # Processar cada imagem
            for i, image in enumerate(images[processed:], start=processed):
                # Verificar se a tarefa foi pausada ou cancelada
                if task_id not in tasks_db:
                    logger.warning(f"⚠️ Tarefa {task_id} não existe mais")
                    return
                
                current_status = tasks_db[task_id].get("status")
                
                if current_status in ["paused", "cancelled"]:
                    logger.info(f"🛑 Tarefa {task_id} foi {current_status}")
                    return
                
                try:
                    # Gerar novo nome (SEM extensão ainda)
                    new_filename = render_rename_template(template, image)
                    
                    # Pegar nome atual
                    current_filename = image.get('filename', '')
                    
                    # USAR URL DIRETA DO FRONTEND
                    image_url = image.get('src') or image.get('url')
                    
                    if not image_url:
                        raise Exception(f"URL da imagem não fornecida para imagem {image.get('id')}")
                    
                    logger.info(f"📥 Baixando imagem de: {image_url[:100]}...")
                    
                    # PASSO 1: Baixar a imagem da URL original
                    img_response = await client.get(image_url, timeout=30.0)
                    if img_response.status_code != 200:
                        raise Exception(f"Erro ao baixar imagem: HTTP {img_response.status_code}")
                    
                    image_content = img_response.content
                    logger.info(f"✅ Imagem baixada: {len(image_content)} bytes")
                    
                    # PASSO 2: Processar com Pillow para detectar e preservar formato
                    img_buffer = io.BytesIO(image_content)
                    pil_image = Image.open(img_buffer)
                    
                    # Detectar formato original
                    original_format = pil_image.format or 'PNG'
                    logger.info(f"🎨 Formato detectado pelo Pillow: {original_format}")
                    
                    # Detectar se tem transparência
                    has_transparency = False
                    file_extension = '.jpg'  # Padrão
                    
                    # IMPORTANTE: Verificar pela URL original primeiro
                    if '.png' in image_url.lower():
                        file_extension = '.png'
                        has_transparency = True  # Assumir que PNGs têm transparência
                        logger.info(f"✅ URL indica PNG - preservando como PNG")
                    elif '.webp' in image_url.lower():
                        file_extension = '.webp'
                        if pil_image.mode == 'RGBA':
                            has_transparency = True
                        logger.info(f"📄 URL indica WebP - Mode: {pil_image.mode}")
                    elif '.gif' in image_url.lower():
                        file_extension = '.gif'
                        if 'transparency' in pil_image.info:
                            has_transparency = True
                        logger.info(f"📄 URL indica GIF")
                    else:
                        # Verificar pelo formato detectado pelo Pillow
                        if original_format == 'PNG':
                            # Verificar se tem canal alpha ou transparência
                            if pil_image.mode in ('RGBA', 'LA') or (pil_image.mode == 'P' and 'transparency' in pil_image.info):
                                has_transparency = True
                                file_extension = '.png'
                                logger.info(f"✅ PNG com TRANSPARÊNCIA detectada! Mode: {pil_image.mode}")
                            else:
                                # PNG mas sem transparência
                                file_extension = '.png'
                                logger.info(f"📄 PNG sem transparência. Mode: {pil_image.mode}")
                        elif original_format == 'GIF':
                            if 'transparency' in pil_image.info:
                                has_transparency = True
                            file_extension = '.gif'
                            logger.info(f"📄 GIF detectado. Transparência: {has_transparency}")
                        elif original_format == 'WEBP':
                            if pil_image.mode == 'RGBA':
                                has_transparency = True
                            file_extension = '.webp'
                            logger.info(f"📄 WebP detectado. Mode: {pil_image.mode}")
                        else:
                            # JPEG ou outro formato sem transparência
                            file_extension = '.jpg'
                            logger.info(f"📄 Formato {original_format} detectado")
                    
                    # Se tem transparência, garantir que seja preservada
                    if has_transparency or file_extension == '.png':
                        logger.info(f"🎨 PRESERVANDO TRANSPARÊNCIA")
                        
                        # Garantir modo RGBA para preservar canal alpha
                        if pil_image.mode != 'RGBA':
                            pil_image = pil_image.convert('RGBA')
                            logger.info(f"🔄 Convertido para RGBA para preservar transparência")
                        
                        # Forçar extensão PNG para garantir transparência
                        file_extension = '.png'
                        save_format = 'PNG'
                    else:
                        # Sem transparência, pode ser JPG
                        if pil_image.mode == 'RGBA':
                            # Converter RGBA para RGB se não tem transparência real
                            pil_image = pil_image.convert('RGB')
                            logger.info(f"🔄 Convertido RGBA→RGB (sem transparência real)")
                        save_format = original_format if original_format in ['JPEG', 'PNG', 'GIF', 'WEBP'] else 'JPEG'
                    
                    # Nome final com extensão correta
                    final_new_name = f"{new_filename}{file_extension}"
                    logger.info(f"📝 Nome final: {current_filename} → {final_new_name}")
                    
                    # CORREÇÃO: NÃO PULAR MESMO SE JÁ TIVER O NOME CORRETO
                    # SEMPRE PROCESSAR TODAS AS IMAGENS
                    if new_filename in current_filename or final_new_name == current_filename:
                        logger.info(f"ℹ️ Imagem {image.get('id')} já tem o nome correto, mas será reprocessada mesmo assim")
                        # NÃO FAZ CONTINUE! CONTINUA O PROCESSAMENTO NORMAL
                    
                    # PASSO 3: Salvar imagem processada em buffer
                    output_buffer = io.BytesIO()
                    
                    # Configurações de salvamento otimizadas
                    save_kwargs = {
                        'format': save_format,
                        'optimize': True
                    }
                    
                    if save_format == 'PNG' and has_transparency:
                        # Preservar transparência no PNG
                        save_kwargs['transparency'] = pil_image.info.get('transparency', None)
                        save_kwargs['compress_level'] = 6  # Compressão média
                        logger.info(f"💎 Salvando PNG com transparência preservada")
                    elif save_format in ['JPEG', 'JPG']:
                        save_kwargs['quality'] = 95  # Alta qualidade
                        save_kwargs['format'] = 'JPEG'
                        logger.info(f"📸 Salvando JPEG com qualidade 95")
                    
                    # Salvar imagem no buffer
                    pil_image.save(output_buffer, **save_kwargs)
                    output_buffer.seek(0)
                    
                    # Converter para base64
                    processed_image_bytes = output_buffer.getvalue()
                    image_base64 = base64.b64encode(processed_image_bytes).decode('utf-8')
                    
                    logger.info(f"✅ Imagem processada: {len(processed_image_bytes)} bytes")
                    
                    # IMPORTANTE: Preservar dados originais
                    original_alt = image.get('alt', '')
                    original_position = image.get('position', 1)
                    original_variant_ids = image.get('variant_ids', [])
                    
                    logger.info(f"📋 Preservando: Alt='{original_alt}', Posição={original_position}")
                    
                    # PASSO 4: Criar nova imagem no Shopify
                    logger.info(f"📤 Criando nova imagem no Shopify: {final_new_name}")
                    
                    create_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{image.get('product_id')}/images.json"
                    
                    headers = {
                        'X-Shopify-Access-Token': access_token,
                        'Content-Type': 'application/json'
                    }
                    
                    # Upload via base64 com imagem processada
                    new_image_data = {
                        "image": {
                            "attachment": image_base64,
                            "filename": final_new_name,
                            "alt": original_alt,
                            "position": original_position
                        }
                    }
                    
                    # Se tem variantes associadas, manter
                    if original_variant_ids and len(original_variant_ids) > 0:
                        new_image_data["image"]["variant_ids"] = original_variant_ids
                    
                    create_response = await client.post(
                        create_url,
                        headers=headers,
                        json=new_image_data
                    )
                    
                    if create_response.status_code not in [200, 201]:
                        error_text = create_response.text
                        raise Exception(f"Erro ao criar imagem: {error_text}")
                    
                    created_image = create_response.json().get('image', {})
                    new_image_id = created_image.get('id')
                    
                    # Verificar resultado
                    created_src = created_image.get('src', '')
                    if has_transparency:
                        if '.png' in created_src.lower():
                            logger.info(f"✅ PNG com transparência preservado com sucesso!")
                        else:
                            logger.warning(f"⚠️ Shopify pode ter convertido o formato. Verifique: {created_src[:100]}")
                    
                    logger.info(f"✅ Nova imagem criada com ID: {new_image_id}")
                    
                    # PASSO 5: Deletar imagem antiga
                    logger.info(f"🗑️ Deletando imagem antiga {image.get('id')}")
                    
                    delete_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{image.get('product_id')}/images/{image.get('id')}.json"
                    delete_response = await client.delete(delete_url, headers=headers)
                    
                    if delete_response.status_code not in [200, 204]:
                        logger.warning(f"⚠️ Aviso ao deletar imagem antiga: HTTP {delete_response.status_code}")
                    else:
                        logger.info(f"✅ Imagem antiga deletada")
                    
                    successful += 1
                    
                    # Preparar dados da imagem atualizada
                    updated_image = {
                        'id': new_image_id,
                        'product_id': image.get('product_id'),
                        'position': created_image.get('position'),
                        'alt': original_alt,
                        'width': created_image.get('width'),
                        'height': created_image.get('height'),
                        'src': created_image.get('src'),
                        'url': created_image.get('src'),
                        'filename': final_new_name,
                        'variant_ids': created_image.get('variant_ids', []),
                        'has_transparency': has_transparency,
                        'original_format': original_format
                        # REMOVIDO: 'original_url': image_url  # NÃO ARMAZENAR URL ORIGINAL
                    }
                    
                    results.append({
                        'image_id': image.get('id'),
                        'new_image_id': new_image_id,
                        'product_id': image.get('product_id'),
                        'status': 'success',
                        'old_name': current_filename,
                        'new_name': final_new_name,
                        'updated_image': updated_image,
                        'transparency_preserved': has_transparency
                    })
                    
                    logger.info(f"✅ Renomeação concluída para imagem {image.get('id')}")
                    
                    # Limpar memória
                    pil_image.close()
                    img_buffer.close()
                    output_buffer.close()
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao processar imagem {image.get('id')}: {str(e)}")
                    failed += 1
                    results.append({
                        'image_id': image.get('id'),
                        'product_id': image.get('product_id'),
                        'status': 'failed',
                        'error': str(e),
                        'old_name': current_filename if 'current_filename' in locals() else 'unknown',
                        'new_name': f"{new_filename}{file_extension}" if 'new_filename' in locals() and 'file_extension' in locals() else 'unknown'
                    })
                
                # Atualizar progresso
                processed += 1
                percentage = round((processed / total) * 100)
                
                if task_id in tasks_db:
                    current_image_info = None
                    if processed < total:
                        current_image_info = f"Imagem {image.get('id')} - {image.get('product_title', 'Produto')}"
                    
                    tasks_db[task_id]["progress"] = {
                        "processed": processed,
                        "total": total,
                        "successful": successful,
                        "failed": failed,
                        "unchanged": unchanged,
                        "percentage": percentage,
                        "current_image": current_image_info
                    }
                    tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                    
                    # OTIMIZAÇÃO 2: LIMITAR RESULTS DURANTE O PROCESSO
                    if len(results) > 20:
                        tasks_db[task_id]["results"] = results[-20:]
                    else:
                        tasks_db[task_id]["results"] = results.copy()
                
                # Verificar novamente se foi pausado/cancelado
                if task_id in tasks_db:
                    if tasks_db[task_id].get("status") in ["paused", "cancelled"]:
                        logger.info(f"🛑 Parando após processar imagem {image.get('id')}")
                        return
                
                # Rate limiting
                await asyncio.sleep(1.0)
        
        # Finalizar tarefa
        final_status = "completed" if failed == 0 else "completed_with_errors"
        
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = final_status
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()
            
            # OTIMIZAÇÃO 3: LIMPAR DADOS APÓS CONCLUSÃO
            # Manter apenas últimos 10 results para tarefas completadas
            tasks_db[task_id]["results"] = results[-10:]
            
            # Limpar config desnecessário
            if "config" in tasks_db[task_id]:
                tasks_db[task_id]["config"] = {
                    "template": tasks_db[task_id]["config"].get("template"),
                    "itemCount": tasks_db[task_id]["config"].get("itemCount"),
                    "storeName": tasks_db[task_id]["config"].get("storeName")
                    # REMOVIDO accessToken e outros dados sensíveis
                }
            
            tasks_db[task_id]["progress"]["current_image"] = None
            
            logger.info(f"🏁 PROCESSO DE RENOMEAÇÃO FINALIZADO:")
            logger.info(f"   ✅ Renomeados: {successful}")
            logger.info(f"   ❌ Falhas: {failed}")
            logger.info(f"   ⚪ Inalterados: {unchanged}")
            logger.info(f"   📊 Total: {processed}/{total}")
            
    except Exception as e:
        logger.error(f"❌ Erro crítico no processamento: {str(e)}")
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error"] = str(e)
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()
            
            # OTIMIZAÇÃO 3: LIMPAR DADOS EM CASO DE ERRO TAMBÉM
            if "config" in tasks_db[task_id]:
                tasks_db[task_id]["config"] = {
                    "itemCount": tasks_db[task_id]["config"].get("itemCount", 0)
                }
            tasks_db[task_id]["results"] = []  # Limpar results em caso de erro

def render_rename_template(template: str, image: Dict) -> str:
    """
    Renderizar template de renomeação com os dados da imagem
    """
    
    try:
        result = template
        
        # Substituir variáveis do produto
        result = re.sub(r'\{\{\s*product\.title\s*\}\}', image.get('product_title', 'produto'), result)
        result = re.sub(r'\{\{\s*product\.handle\s*\}\}', image.get('product_handle', 'produto'), result)
        result = re.sub(r'\{\{\s*product\.vendor\s*\}\}', image.get('product_vendor', 'vendor'), result)
        result = re.sub(r'\{\{\s*product\.type\s*\}\}', image.get('product_type', 'type'), result)
        result = re.sub(r'\{\{\s*image\.position\s*\}\}', str(image.get('position', 1)), result)
        
        # CORREÇÃO: USAR variant_data QUE O FRONTEND ESTÁ ENVIANDO!
        variant_data = image.get('variant_data', {})
        
        if variant_data:
            # Usar os dados corretos enviados pelo frontend
            result = re.sub(r'\{\{\s*variant\.name1\s*\}\}', variant_data.get('name1', ''), result)
            result = re.sub(r'\{\{\s*variant\.name2\s*\}\}', variant_data.get('name2', ''), result)
            result = re.sub(r'\{\{\s*variant\.name3\s*\}\}', variant_data.get('name3', ''), result)
            result = re.sub(r'\{\{\s*variant\.value1\s*\}\}', variant_data.get('value1', ''), result)
            result = re.sub(r'\{\{\s*variant\.value2\s*\}\}', variant_data.get('value2', ''), result)
            result = re.sub(r'\{\{\s*variant\.value3\s*\}\}', variant_data.get('value3', ''), result)
        else:
            # Limpar variáveis de variante se não houver
            result = re.sub(r'\{\{\s*variant\.name[1-3]\s*\}\}', '', result)
            result = re.sub(r'\{\{\s*variant\.value[1-3]\s*\}\}', '', result)
        
        # Limpar e formatar o resultado final
        result = result.strip()
        result = re.sub(r'\s+', '-', result)  # Espaços para hífens
        result = re.sub(r'[^a-zA-Z0-9\-]', '', result)  # Remover caracteres especiais
        result = re.sub(r'--+', '-', result)  # Múltiplos hífens para um
        result = re.sub(r'^-|-$', '', result)  # Remover hífens do início e fim
        result = result.lower()  # Converter para minúsculas
        
        # Se o resultado estiver vazio, usar um nome padrão
        if not result:
            result = f"image-{image.get('id', 'unknown')}"
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Erro ao renderizar template: {str(e)}")
        return f"image-{image.get('id', 'unknown')}-{int(datetime.now().timestamp())}"

# ==================== ENDPOINT DE AGENDAMENTO DE RENOMEAÇÃO ====================

@app.post("/api/rename/schedule")
async def schedule_rename_task(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Endpoint específico para agendar tarefas de renomeação
    Suporta todas as funcionalidades de agendamento, notificações e execução programada
    """
    
    task_id = data.get("id") or f"scheduled_rename_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    logger.info(f"📋 Recebendo agendamento de renomeação: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    scheduled_for = data.get("scheduled_for", get_brazil_time_str())
    
    # Processar timezone corretamente
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
    
    logger.info(f"📅 Horário convertido para local: {scheduled_time_naive}")
    logger.info(f"📅 Horário atual do servidor: {now}")
    
    # Processar notificações se configuradas
    notification_scheduled_for = None
    notification_config = data.get("notifications", {})
    
    if notification_config and notification_config.get("before_execution"):
        notification_time_minutes = notification_config.get("notification_time", 30)
        
        # Calcular horário da notificação
        notification_datetime = scheduled_time_naive - timedelta(minutes=notification_time_minutes)
        notification_scheduled_for = notification_datetime.isoformat()
        
        logger.info(f"📱 Notificação configurada para: {notification_datetime}")
        logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
    
    # Verificar se deve executar imediatamente ou agendar
    if scheduled_time_naive <= now:
        logger.info(f"📅 Tarefa de renomeação {task_id} agendada para horário passado, executando imediatamente!")
        
        # Criar tarefa com status processing
        task = {
            "id": task_id,
            "name": data.get("name", "Renomeação de Imagens"),
            "task_type": "rename_images",
            "status": "processing",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": notification_config
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "unchanged": 0,
                "percentage": 0,
                "current_image": None
            },
            "results": []
        }
        
        tasks_db[task_id] = task
        
        # Processar imediatamente em background
        config = task.get("config", {})
        background_tasks.add_task(
            process_rename_images_background,
            task_id,
            config.get("template", ""),
            config.get("images", []),
            config.get("storeName", ""),
            config.get("accessToken", "")
        )
        
        logger.info(f"▶️ Tarefa de renomeação {task_id} iniciada imediatamente")
        
        return {
            "success": True,
            "taskId": task_id,
            "task": task,
            "message": "Tarefa iniciada imediatamente (horário já passou)",
            "execution": "immediate"
        }
    else:
        # Agendar para execução futura
        task = {
            "id": task_id,
            "name": data.get("name", "Renomeação de Imagens"),
            "task_type": "rename_images",
            "status": "scheduled",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": notification_config
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "unchanged": 0,
                "percentage": 0,
                "current_image": None
            },
            "results": []
        }
        
        tasks_db[task_id] = task
        
        # Calcular tempo restante
        diff = (scheduled_time_naive - now).total_seconds()
        hours = int(diff // 3600)
        minutes = int((diff % 3600) // 60)
        
        time_msg = f"{hours}h {minutes}min" if hours > 0 else f"{minutes} minutos"
        
        logger.info(f"📅 Tarefa de renomeação {task_id} agendada para {scheduled_time_naive}")
        logger.info(f"⏱️ Será executada em {time_msg}")
        
        return {
            "success": True,
            "taskId": task_id,
            "task": task,
            "message": f"Tarefa agendada com sucesso para execução em {time_msg}",
            "execution": "scheduled",
            "scheduled_time": scheduled_time_naive.isoformat(),
            "time_remaining": {
                "seconds": int(diff),
                "minutes": minutes + (hours * 60),
                "hours": hours,
                "formatted": time_msg
            }
        }

# ==================== ENDPOINTS DE OTIMIZAÇÃO DE IMAGENS ====================

@app.post("/api/images/optimize")
async def optimize_images(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Endpoint para otimizar imagens (redimensionar mantendo proporção)
    Preserva: nome do arquivo, transparência, alt-text, ordem das imagens
    """
    
    task_id = data.get("id") or f"optimize_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    # PEGAR targetHeight DO FRONTEND!
    target_height = data.get("targetHeight")
    if not target_height:
        raise HTTPException(status_code=400, detail="targetHeight não fornecido")
    
    logger.info(f"📋 Nova tarefa de otimização de imagens {task_id}")
    logger.info(f"🎯 Altura alvo: {target_height}px (recebido do frontend)")
    logger.info(f"📸 Imagens para processar: {len(data.get('images', []))}")
    
    images = data.get("images", [])
    store_name = data.get("storeName", "")
    access_token = data.get("accessToken", "")
    
    if not images:
        raise HTTPException(status_code=400, detail="Nenhuma imagem para otimizar")
    if not store_name or not access_token:
        raise HTTPException(status_code=400, detail="Credenciais da loja não fornecidas")
    
    # Salvar tarefa na memória
    tasks_db[task_id] = {
        "id": task_id,
        "name": f"Otimização - {len(images)} imagens para {target_height}px",
        "status": "processing",
        "task_type": "image_optimization",
        "progress": {
            "processed": 0,
            "total": len(images),
            "successful": 0,
            "failed": 0,
            "percentage": 0,
            "current_image": None
        },
        "started_at": get_brazil_time_str(),
        "updated_at": get_brazil_time_str(),
        "config": {
            "targetHeight": target_height,
            "storeName": store_name,
            "accessToken": access_token,
            "itemCount": len(images),
            "images": images  # IMPORTANTE: Salvar imagens no config para retomada
        },
        "settings": {
            "targetHeight": target_height
        },
        "results": []
    }
    
    logger.info(f"✅ Tarefa de otimização {task_id} iniciada")
    
    # Processar em background
    background_tasks.add_task(
        process_image_optimization_background,
        task_id,
        images,
        target_height,
        store_name,
        access_token
    )
    
    return {
        "success": True,
        "message": f"Otimização iniciada para {len(images)} imagens",
        "taskId": task_id,
        "estimatedTime": f"{len(images) * 2:.1f} segundos",
        "mode": "background_processing"
    }

async def process_image_optimization_background(
    task_id: str,
    images: List[Dict],
    target_height: int,
    store_name: str,
    access_token: str,
    is_resume: bool = False
):
    """
    Processar otimização de imagens - FLUXO MELHORADO
    Download -> Otimizar -> Deletar antiga (com retry) -> Upload com mesmo nome
    """
    
    try:
        from PIL import Image
        import io
        import base64
        from urllib.parse import urlparse, unquote
        import os
        import numpy as np
        
        def has_real_transparency(pil_image):
            """
            Detecta se a imagem tem TRANSPARÊNCIA REAL (não apenas um canal alpha)
            """
            
            # Se não tem canal alpha, não tem transparência
            if pil_image.mode not in ('RGBA', 'LA', 'PA'):
                return False
            
            # Converter para RGBA se necessário
            if pil_image.mode != 'RGBA':
                pil_image = pil_image.convert('RGBA')
            
            # Converter para numpy array
            img_array = np.array(pil_image)
            
            # Pegar o canal alpha
            if len(img_array.shape) == 3 and img_array.shape[2] == 4:
                alpha_channel = img_array[:, :, 3]
            else:
                return False
            
            # Análises do canal alpha
            unique_alpha_values = np.unique(alpha_channel)
            
            # Se todos os pixels são 100% opacos (255), não tem transparência real
            if len(unique_alpha_values) == 1 and unique_alpha_values[0] == 255:
                logger.info("❌ Falso positivo: Imagem tem canal alpha mas todos pixels são opacos")
                return False
            
            # Calcular porcentagem de pixels transparentes/semi-transparentes
            total_pixels = alpha_channel.size
            transparent_pixels = np.sum(alpha_channel < 255)
            transparency_ratio = transparent_pixels / total_pixels
            
            logger.info(f"📊 Análise de transparência:")
            logger.info(f"   - Pixels transparentes: {transparent_pixels}/{total_pixels} ({transparency_ratio*100:.1f}%)")
            
            # Se menos de 1% dos pixels são transparentes, provavelmente é ruído
            if transparency_ratio < 0.01:
                logger.info("❌ Transparência insignificante (<1%), tratando como opaca")
                return False
            
            # Verificar se a transparência forma uma "moldura" (comum em logos/ícones)
            edge_alpha = np.concatenate([
                alpha_channel[0, :],      # topo
                alpha_channel[-1, :],     # baixo
                alpha_channel[:, 0],      # esquerda
                alpha_channel[:, -1]      # direita
            ])
            
            edge_transparent = np.sum(edge_alpha < 255) / edge_alpha.size
            
            # Se mais de 50% das bordas são transparentes, provavelmente é transparência real
            if edge_transparent > 0.5:
                logger.info("✅ Transparência real detectada (bordas transparentes)")
                return True
            
            # Verificar complexidade da transparência
            opaque_mask = alpha_channel == 255
            horizontal_changes = np.sum(np.diff(opaque_mask.astype(int), axis=1) != 0)
            vertical_changes = np.sum(np.diff(opaque_mask.astype(int), axis=0) != 0)
            
            total_changes = horizontal_changes + vertical_changes
            complexity_ratio = total_changes / total_pixels
            
            logger.info(f"   - Complexidade da transparência: {complexity_ratio:.4f}")
            
            if complexity_ratio > 0.001:
                logger.info("✅ Transparência complexa detectada (provavelmente intencional)")
                return True
            
            if transparency_ratio < 0.1:
                logger.info("❌ Pouca transparência, tratando como imagem opaca")
                return False
            
            logger.info("✅ Transparência significativa detectada")
            return True
        
        def should_preserve_as_png(pil_image, image_url):
            """
            Decide se deve preservar como PNG baseado em análise inteligente
            """
            
            # Verificar extensão original
            is_png_originally = '.png' in image_url.lower()
            
            # Verificar se tem transparência REAL
            has_transparency = has_real_transparency(pil_image)
            
            # Se era PNG mas não tem transparência real, pode converter para JPG
            if is_png_originally and not has_transparency:
                logger.info("🔄 PNG original mas sem transparência real - convertendo para JPG")
                return False
            
            # Se tem transparência real, manter como PNG
            if has_transparency:
                logger.info("💎 Mantendo como PNG - transparência real detectada")
                return True
            
            # Verificar se é uma imagem de produto (geralmente não precisa transparência)
            width, height = pil_image.size
            aspect_ratio = width / height if height > 0 else 1
            
            # Imagens quadradas pequenas são geralmente logos/ícones
            if width < 500 and height < 500 and 0.8 < aspect_ratio < 1.2:
                logger.info("🎨 Possível logo/ícone detectado - verificando transparência")
                return has_transparency
            
            # Imagens grandes de produto geralmente não precisam transparência
            if width > 1000 or height > 1000:
                logger.info("📸 Imagem grande de produto - usando JPG")
                return False
            
            return has_transparency
        
        if not is_resume:
            logger.info(f"🚀 INICIANDO OTIMIZAÇÃO DIRETA DE IMAGENS: {task_id}")
        else:
            logger.info(f"▶️ RETOMANDO OTIMIZAÇÃO: {task_id}")
        
        logger.info(f"🎯 Altura alvo: {target_height}px")
        logger.info(f"📸 Total de imagens: {len(images)}")
        
        clean_store = store_name.replace('.myshopify.com', '').strip()
        api_version = '2024-01'
        
        # CORREÇÃO IMPORTANTE: Gerenciar progresso corretamente
        if is_resume and task_id in tasks_db:
            task = tasks_db[task_id]
            processed = task["progress"]["processed"]
            successful = task["progress"]["successful"]
            failed = task["progress"]["failed"]
            results = task.get("results", [])
            total = task["progress"]["total"]
            
            logger.info(f"📊 Retomando do ponto: {processed}/{total} já processadas")
            
            # PULAR IMAGENS JÁ PROCESSADAS
            start_index = processed
        else:
            processed = 0
            successful = 0
            failed = 0
            results = []
            total = len(images)
            start_index = 0
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # CORREÇÃO: Usar enumerate com start correto
            for idx, image in enumerate(images):
                # PULAR IMAGENS JÁ PROCESSADAS SE FOR RETOMADA
                if idx < start_index:
                    continue
                
                # Verificar se foi pausado/cancelado
                if task_id not in tasks_db:
                    logger.warning(f"⚠️ Tarefa {task_id} não existe mais")
                    return
                
                current_status = tasks_db[task_id].get("status")
                
                if current_status in ["paused", "cancelled"]:
                    logger.info(f"🛑 Tarefa {task_id} foi {current_status}")
                    return
                
                try:
                    # Informações da imagem original
                    image_url = image.get('src') or image.get('url')
                    original_alt = image.get('alt', '')
                    original_position = image.get('position', 1)
                    original_width = image.get('dimensions', {}).get('width', 0)
                    original_height = image.get('dimensions', {}).get('height', 0)
                    product_id = image.get('product_id')
                    image_id = image.get('id')
                    variant_ids = image.get('variant_ids', [])
                    
                    # Extrair nome limpo do arquivo
                    parsed_url = urlparse(image_url)
                    path_parts = parsed_url.path.split('/')
                    
                    original_filename = None
                    for part in reversed(path_parts):
                        if part and '.' in part:
                            original_filename = unquote(part.split('?')[0])
                            
                            # Remover sufixo UUID/hash se existir
                            if '_' in original_filename:
                                name, ext = os.path.splitext(original_filename)
                                parts = name.rsplit('_', 1)
                                
                                if len(parts) == 2:
                                    suffix = parts[1]
                                    has_numbers = any(c.isdigit() for c in suffix)
                                    has_letters = any(c.isalpha() for c in suffix)
                                    
                                    if (has_numbers and has_letters) or len(suffix) > 10:
                                        original_filename = parts[0] + ext
                                        logger.info(f"🔪 Removido sufixo: _{suffix}")
                            break
                    
                    if not original_filename:
                        original_filename = f"product-image-{image_id}.jpg"
                    
                    # CORREÇÃO: Mostrar progresso correto
                    current_progress = processed + 1
                    logger.info(f"📥 Processando imagem {current_progress}/{total}: {original_filename}")
                    
                    # Verificar se precisa otimização
                    if original_height <= target_height:
                        logger.info(f"✅ Imagem já está no tamanho adequado ({original_height}px ≤ {target_height}px)")
                        processed += 1
                        successful += 1
                        
                        # Atualizar progresso
                        if task_id in tasks_db:
                            percentage = round((processed / total) * 100)
                            remaining = total - processed
                            tasks_db[task_id]["progress"] = {
                                "processed": processed,
                                "total": total,
                                "successful": successful,
                                "failed": failed,
                                "percentage": percentage,
                                "remaining": remaining,
                                "current_image": f"Processando imagens... {processed}/{total}"
                            }
                            tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                        
                        continue
                    
                    # ============ PASSO 1: DOWNLOAD ============
                    img_response = await client.get(image_url, timeout=30.0)
                    if img_response.status_code != 200:
                        raise Exception(f"Erro ao baixar imagem: HTTP {img_response.status_code}")
                    
                    image_content = img_response.content
                    logger.info(f"✅ Imagem baixada: {len(image_content)} bytes")
                    
                    # ============ PASSO 2: OTIMIZAÇÃO ============
                    img_buffer = io.BytesIO(image_content)
                    pil_image = Image.open(img_buffer)
                    
                    # Análise inteligente de transparência
                    logger.info(f"🔍 Analisando transparência da imagem...")
                    should_be_png = should_preserve_as_png(pil_image, image_url)
                    
                    # Calcular novas dimensões
                    ratio = original_width / original_height
                    new_height = target_height
                    new_width = int(new_height * ratio)
                    
                    logger.info(f"🔄 Redimensionando: {original_width}x{original_height} → {new_width}x{new_height}")
                    
                    # Redimensionar baseado na análise
                    if should_be_png:
                        # Preservar transparência
                        if pil_image.mode != 'RGBA':
                            pil_image = pil_image.convert('RGBA')
                        resized_image = pil_image.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        save_format = 'PNG'
                        file_extension = '.png'
                        
                        # Verificar novamente após redimensionamento
                        if not has_real_transparency(resized_image):
                            logger.info("⚠️ Transparência perdida no redimensionamento, convertendo para JPG")
                            resized_image = resized_image.convert('RGB')
                            save_format = 'JPEG'
                            file_extension = '.jpg'
                    else:
                        # Converter para JPG (sem transparência)
                        if pil_image.mode == 'RGBA':
                            # Criar fundo branco para áreas transparentes
                            background = Image.new('RGB', pil_image.size, (255, 255, 255))
                            background.paste(pil_image, mask=pil_image.split()[3] if len(pil_image.split()) > 3 else None)
                            pil_image = background
                        elif pil_image.mode != 'RGB':
                            pil_image = pil_image.convert('RGB')
                        
                        resized_image = pil_image.resize((new_width, new_height), Image.Resampling.LANCZOS)
                        save_format = 'JPEG'
                        file_extension = '.jpg'
                    
                    # Salvar imagem otimizada
                    output_buffer = io.BytesIO()
                    
                    save_kwargs = {
                        'format': save_format,
                        'optimize': True
                    }
                    
                    if save_format == 'PNG':
                        save_kwargs['compress_level'] = 6
                        if should_be_png:
                            save_kwargs['transparency'] = pil_image.info.get('transparency', None)
                        logger.info(f"💎 Salvando como PNG com transparência preservada")
                    else:
                        save_kwargs['quality'] = 90
                        logger.info(f"📸 Salvando como JPEG (sem transparência desnecessária)")
                    
                    resized_image.save(output_buffer, **save_kwargs)
                    output_buffer.seek(0)
                    optimized_bytes = output_buffer.getvalue()
                    
                    # Calcular economia
                    original_size = len(image_content)
                    optimized_size = len(optimized_bytes)
                    savings_percentage = round(((original_size - optimized_size) / original_size) * 100)
                    
                    logger.info(f"✅ Imagem otimizada: {optimized_size} bytes ({savings_percentage}% menor)")
                    
                    # Ajustar nome do arquivo
                    base_name = os.path.splitext(original_filename)[0]
                    new_filename = f"{base_name}{file_extension}"
                    
                    # ============ PASSO 3: DELETAR ORIGINAL PRIMEIRO (FLUXO MELHORADO) ============
                    headers = {
                        'X-Shopify-Access-Token': access_token,
                        'Content-Type': 'application/json'
                    }
                    
                    delete_success = False
                    delete_attempts = 0
                    max_delete_attempts = 3
                    
                    logger.info(f"🗑️ Tentando deletar imagem original {image_id} ANTES do upload...")
                    
                    while not delete_success and delete_attempts < max_delete_attempts:
                        try:
                            delete_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}/images/{image_id}.json"
                            delete_response = await client.delete(delete_url, headers=headers)
                            
                            if delete_response.status_code in [200, 204]:
                                logger.info(f"✅ Imagem original deletada com sucesso (tentativa {delete_attempts + 1})")
                                delete_success = True
                            elif delete_response.status_code == 404:
                                logger.info(f"⚠️ Imagem original já não existe (404)")
                                delete_success = True  # Considerar sucesso se já não existe
                            else:
                                logger.warning(f"⚠️ Falha ao deletar (tentativa {delete_attempts + 1}): HTTP {delete_response.status_code}")
                                delete_attempts += 1
                                if delete_attempts < max_delete_attempts:
                                    await asyncio.sleep(1)  # Aguardar 1 segundo antes de tentar novamente
                        except Exception as del_error:
                            logger.warning(f"⚠️ Erro ao deletar (tentativa {delete_attempts + 1}): {str(del_error)}")
                            delete_attempts += 1
                            if delete_attempts < max_delete_attempts:
                                await asyncio.sleep(1)
                    
                    # ============ PASSO 4: UPLOAD DA NOVA IMAGEM ============
                    logger.info(f"📤 Enviando imagem otimizada para Shopify com nome: {new_filename}")
                    
                    # Converter para base64
                    image_base64 = base64.b64encode(optimized_bytes).decode('utf-8')
                    
                    # Criar nova imagem
                    create_url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}/images.json"
                    
                    create_data = {
                        "image": {
                            "attachment": image_base64,
                            "filename": new_filename,
                            "alt": original_alt,
                            "position": original_position
                        }
                    }
                    
                    # Se tem variantes associadas, manter
                    if variant_ids and len(variant_ids) > 0:
                        create_data["image"]["variant_ids"] = variant_ids
                    
                    create_response = await client.post(
                        create_url,
                        headers=headers,
                        json=create_data
                    )
                    
                    if create_response.status_code not in [200, 201]:
                        error_text = create_response.text
                        raise Exception(f"Erro ao criar imagem: {error_text}")
                    
                    created_image = create_response.json().get('image', {})
                    new_image_id = created_image.get('id')
                    
                    logger.info(f"✅ Nova imagem criada com ID: {new_image_id}")
                    
                    # ============ PASSO 5: SE DELETAR FALHOU ANTES, TENTAR NOVAMENTE ============
                    if not delete_success:
                        logger.info(f"🗑️ Tentando deletar imagem original novamente (pós-upload)...")
                        try:
                            delete_response = await client.delete(delete_url, headers=headers)
                            if delete_response.status_code in [200, 204]:
                                logger.info(f"✅ Imagem original finalmente deletada")
                            else:
                                logger.warning(f"⚠️ Não foi possível deletar imagem original: HTTP {delete_response.status_code}")
                                logger.warning(f"⚠️ Pode haver duplicata temporária até limpeza manual")
                        except Exception as final_del_error:
                            logger.warning(f"⚠️ Erro final ao tentar deletar: {str(final_del_error)}")
                    
                    successful += 1
                    
                    results.append({
                        'image_id': image_id,
                        'new_image_id': new_image_id,
                        'product_id': product_id,
                        'status': 'success',
                        'old_size': original_size,
                        'new_size': optimized_size,
                        'savings': savings_percentage,
                        'dimensions': f"{new_width}x{new_height}",
                        'transparency_preserved': should_be_png,
                        'original_deleted': delete_success
                    })
                    
                    # Limpar memória
                    pil_image.close()
                    resized_image.close()
                    img_buffer.close()
                    output_buffer.close()
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao processar imagem: {str(e)}")
                    failed += 1
                    results.append({
                        'image_id': image.get('id'),
                        'product_id': image.get('product_id'),
                        'status': 'failed',
                        'error': str(e)
                    })
                
                # IMPORTANTE: Incrementar processed SEMPRE
                processed += 1
                
                # Atualizar progresso
                if task_id in tasks_db:
                    percentage = round((processed / total) * 100)
                    
                    # Calcular restantes corretamente
                    remaining = total - processed
                    
                    tasks_db[task_id]["progress"] = {
                        "processed": processed,
                        "total": total,
                        "successful": successful,
                        "failed": failed,
                        "percentage": percentage,
                        "remaining": remaining,  # Adicionar campo remaining
                        "current_image": f"Processando imagens... {processed}/{total}"
                    }
                    tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                    
                    # Limitar results para economizar memória
                    if len(results) > 20:
                        tasks_db[task_id]["results"] = results[-20:]
                    else:
                        tasks_db[task_id]["results"] = results.copy()
                
                # Verificar se foi pausado/cancelado novamente
                if task_id in tasks_db:
                    if tasks_db[task_id].get("status") in ["paused", "cancelled"]:
                        logger.info(f"🛑 Tarefa {task_id} foi {tasks_db[task_id].get('status')}")
                        return
                
                # Rate limiting
                await asyncio.sleep(0.5)
        
        # Finalizar
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = "completed" if failed == 0 else "completed_with_errors"
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()
            tasks_db[task_id]["results"] = results[-10:]
            
            logger.info(f"🏁 OTIMIZAÇÃO FINALIZADA:")
            logger.info(f"   ✅ Processadas: {successful}")
            logger.info(f"   ❌ Falhas: {failed}")
            logger.info(f"   📊 Total: {processed}/{total}")
            
    except Exception as e:
        logger.error(f"❌ Erro crítico: {str(e)}")
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error"] = str(e)
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()

@app.post("/api/images/schedule-optimization")
async def schedule_image_optimization(data: Dict[str, Any], background_tasks: BackgroundTasks):
    """
    Agendar tarefa de otimização de imagens
    Suporta todas as funcionalidades de agendamento e notificações
    """
    
    task_id = data.get("id") or f"scheduled_optimize_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    logger.info(f"📋 Recebendo agendamento de otimização: {data.get('name')}")
    logger.info(f"⏰ Para executar em: {data.get('scheduled_for')}")
    
    scheduled_for = data.get("scheduled_for", get_brazil_time_str())
    
    # Processar timezone
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
    
    logger.info(f"📅 Horário convertido para local: {scheduled_time_naive}")
    logger.info(f"📅 Horário atual do servidor: {now}")
    
    # Processar notificações se configuradas
    notification_scheduled_for = None
    notification_config = data.get("notifications", {})
    
    if notification_config and notification_config.get("before_execution"):
        notification_time_minutes = notification_config.get("notification_time", 30)
        
        # Calcular horário da notificação
        notification_datetime = scheduled_time_naive - timedelta(minutes=notification_time_minutes)
        notification_scheduled_for = notification_datetime.isoformat()
        
        logger.info(f"📱 Notificação configurada para: {notification_datetime}")
        logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
    
    # PEGAR targetHeight DO CONFIG!
    target_height = data.get("config", {}).get("targetHeight")
    if not target_height:
        raise HTTPException(status_code=400, detail="targetHeight não fornecido no config")
    
    # Verificar se deve executar imediatamente ou agendar
    if scheduled_time_naive <= now:
        logger.info(f"📅 Tarefa de otimização {task_id} agendada para horário passado, executando imediatamente!")
        
        # Criar tarefa com status processing
        task = {
            "id": task_id,
            "name": data.get("name", "Otimização de Imagens"),
            "task_type": "image_optimization",
            "status": "processing",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": notification_config
            },
            "settings": {
                "targetHeight": target_height
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0,
                "current_image": None
            },
            "results": []
        }
        
        tasks_db[task_id] = task
        
        # Processar imediatamente em background
        config = task.get("config", {})
        background_tasks.add_task(
            process_image_optimization_background,
            task_id,
            config.get("images", []),
            target_height,  # USAR O targetHeight DO CONFIG
            config.get("storeName", ""),
            config.get("accessToken", "")
        )
        
        logger.info(f"▶️ Tarefa de otimização {task_id} iniciada imediatamente")
        
        return {
            "success": True,
            "taskId": task_id,
            "task": task,
            "message": "Tarefa iniciada imediatamente (horário já passou)",
            "execution": "immediate"
        }
    else:
        # Agendar para execução futura
        task = {
            "id": task_id,
            "name": data.get("name", "Otimização de Imagens"),
            "task_type": "image_optimization",
            "status": "scheduled",
            "scheduled_for": scheduled_for,
            "scheduled_for_local": scheduled_time_naive.isoformat(),
            "notification_scheduled_for": notification_scheduled_for,
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": notification_config
            },
            "settings": {
                "targetHeight": target_height
            },
            "created_at": get_brazil_time_str(),
            "updated_at": get_brazil_time_str(),
            "progress": {
                "processed": 0,
                "total": data.get("config", {}).get("itemCount", 0),
                "successful": 0,
                "failed": 0,
                "percentage": 0,
                "current_image": None
            },
            "results": []
        }
        
        tasks_db[task_id] = task
        
        # Calcular tempo restante
        diff = (scheduled_time_naive - now).total_seconds()
        hours = int(diff // 3600)
        minutes = int((diff % 3600) // 60)
        
        time_msg = f"{hours}h {minutes}min" if hours > 0 else f"{minutes} minutos"
        
        logger.info(f"📅 Tarefa de otimização {task_id} agendada para {scheduled_time_naive}")
        logger.info(f"⏱️ Será executada em {time_msg}")
        
        return {
            "success": True,
            "taskId": task_id,
            "task": task,
            "message": f"Tarefa agendada com sucesso para execução em {time_msg}",
            "execution": "scheduled",
            "scheduled_time": scheduled_time_naive.isoformat(),
            "time_remaining": {
                "seconds": int(diff),
                "minutes": minutes + (hours * 60),
                "hours": hours,
                "formatted": time_msg
            }
        }

# ==================== UPLOAD DE IMAGEM NA DESCRIÇÃO DE PRODUTOS ====================

@app.post("/api/images/upload-to-shopify")
async def upload_image_to_shopify(data: Dict[str, Any]):
    """
    Upload para ImgBB - GRÁTIS e ILIMITADO
    Não sobrecarrega o Railway, não cria produtos no Shopify
    """
    
    try:
        logger.info("📸 Upload de imagem via ImgBB")
        
        # Extrair dados
        image_base64 = data.get("image")
        filename = data.get("filename", "image.jpg")
        
        if not image_base64:
            return {"success": False, "message": "Nenhuma imagem fornecida"}
        
        # Processar base64
        if "," in image_base64:
            _, image_base64_clean = image_base64.split(",", 1)
        else:
            image_base64_clean = image_base64
        
        logger.info(f"📦 Processando imagem: {filename}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # IMGBB - Serviço GRÁTIS de hospedagem
            # API Key pública (você pode criar a sua própria grátis em imgbb.com)
            imgbb_url = "https://api.imgbb.com/1/upload"
            
            # Preparar dados
            form_data = {
                'key': '6d207e02198a847aa98d0a2a901485a5',  # API key pública
                'image': image_base64_clean,
                'name': filename.split('.')[0]
            }
            
            # Fazer upload
            response = await client.post(imgbb_url, data=form_data)
            
            if response.status_code == 200:
                result = response.json()
                
                if result.get('success') and result.get('data'):
                    image_data = result['data']
                    
                    # URL permanente do ImgBB
                    permanent_url = image_data.get('url')
                    
                    logger.info(f"✅ Upload concluído: {permanent_url}")
                    
                    return {
                        "success": True,
                        "url": permanent_url,
                        "display_url": image_data.get('display_url'),
                        "delete_url": image_data.get('delete_url'),
                        "filename": filename,
                        "size": image_data.get('size', 0),
                        "width": image_data.get('width'),
                        "height": image_data.get('height'),
                        "permanent": True,
                        "service": "ImgBB",
                        "message": "Upload concluído com sucesso"
                    }
                else:
                    logger.error(f"❌ ImgBB retornou erro: {result}")
                    raise Exception("ImgBB rejeitou o upload")
            else:
                logger.error(f"❌ HTTP {response.status_code}: {response.text}")
                raise Exception(f"Erro HTTP {response.status_code}")
                
    except Exception as e:
        logger.error(f"❌ Erro no upload: {str(e)}")
        
        # FALLBACK: Tentar Imgur como alternativa
        try:
            logger.info("🔄 Tentando Imgur como fallback...")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                imgur_url = "https://api.imgur.com/3/image"
                
                headers = {
                    'Authorization': 'Client-ID 0b711b9fc2d5a0d'  # Client ID público
                }
                
                form_data = {
                    'image': image_base64_clean,
                    'type': 'base64',
                    'name': filename
                }
                
                response = await client.post(imgur_url, headers=headers, data=form_data)
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result.get('success') and result.get('data'):
                        imgur_data = result['data']
                        imgur_url = imgur_data.get('link')
                        
                        logger.info(f"✅ Upload via Imgur concluído: {imgur_url}")
                        
                        return {
                            "success": True,
                            "url": imgur_url,
                            "delete_hash": imgur_data.get('deletehash'),
                            "filename": filename,
                            "size": imgur_data.get('size', 0),
                            "width": imgur_data.get('width'),
                            "height": imgur_data.get('height'),
                            "permanent": True,
                            "service": "Imgur",
                            "message": "Upload concluído via Imgur"
                        }
                        
        except Exception as imgur_error:
            logger.error(f"❌ Imgur também falhou: {imgur_error}")
        
        return {
            "success": False,
            "message": f"Todos os serviços falharam: {str(e)}"
        }

# ==================== ENDPOINTS DE NOTIFICAÇÕES (NOVOS) ====================

@app.get("/api/notifications/pending")
async def get_pending_notifications():
    """Retornar notificações pendentes para exibição"""
    
    now = datetime.now()
    pending_notifications = []
    
    for task_id, task in tasks_db.items():
        if task.get("status") == "scheduled":
            # Verificar se tem notificação configurada
            if task.get("notification_scheduled_for"):
                notification_time = datetime.fromisoformat(
                    task["notification_scheduled_for"].replace('Z', '')
                )
                
                # Pegar horário da tarefa
                task_time_str = task.get("scheduled_for_local") or task.get("scheduled_for")
                if task_time_str.endswith('Z'):
                    task_time = datetime.fromisoformat(task_time_str[:-1])
                else:
                    task_time = datetime.fromisoformat(task_time_str.replace('Z', ''))
                
                # Se está no período de notificação (passou da hora de notificar mas ainda não executou)
                if notification_time <= now < task_time:
                    # Verificar se já foi enviada/dispensada
                    if not task.get("config", {}).get("notifications", {}).get("before_execution_sent"):
                        pending_notifications.append({
                            "task_id": task_id,
                            "task_name": task.get("name"),
                            "task_type": task.get("task_type"),
                            "scheduled_for": task.get("scheduled_for"),
                            "notification_time": task.get("notification_scheduled_for"),
                            "priority": task.get("priority"),
                            "minutes_before": task.get("config", {}).get("notifications", {}).get("notification_time"),
                            "item_count": task.get("config", {}).get("itemCount") or task.get("progress", {}).get("total"),
                            "description": task.get("description")
                        })
                        
                        logger.info(f"📱 Notificação pendente para tarefa {task_id}: {task.get('name')}")
    
    logger.info(f"📱 Total de {len(pending_notifications)} notificações pendentes")
    
    return {
        "notifications": pending_notifications,
        "count": len(pending_notifications),
        "server_time": now.isoformat()
    }

@app.post("/api/notifications/dismiss/{task_id}")
async def dismiss_notification(task_id: str):
    """Marcar notificação como dispensada/visualizada"""
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail=f"Tarefa {task_id} não encontrada")
    
    task = tasks_db[task_id]
    
    # Marcar notificação como enviada/dispensada
    if "config" not in task:
        task["config"] = {}
    if "notifications" not in task["config"]:
        task["config"]["notifications"] = {}
    
    task["config"]["notifications"]["before_execution_sent"] = True
    task["config"]["notifications"]["dismissed_at"] = get_brazil_time_str()
    task["updated_at"] = get_brazil_time_str()
    
    logger.info(f"🔕 Notificação da tarefa {task_id} marcada como dispensada")
    
    return {
        "success": True,
        "message": "Notificação dispensada",
        "task_id": task_id
    }

# ==================== WEBSOCKET PARA ATUALIZAÇÕES EM TEMPO REAL ====================

@app.websocket("/ws/images")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

async def notify_image_update(product_ids: List[str], task_type: str):
    """Notificar clientes conectados sobre atualização de imagens"""
    await manager.broadcast({
        "type": "image_update",
        "task_type": task_type,
        "product_ids": product_ids,
        "timestamp": get_brazil_time_str()
    })

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
            "completed": sum(1 for t in tasks_db.values() if t["status"] == "completed"),
            "completed_with_errors": sum(1 for t in tasks_db.values() if t["status"] == "completed_with_errors"),
            "failed": sum(1 for t in tasks_db.values() if t["status"] == "failed"),
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
            "csvContent": csv_content,
            "productIds": product_ids,
            "submitData": submit_data,
            "storeName": store_name,
            "accessToken": access_token,
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
    access_token: str,
    is_resume: bool = False
):
    """Processar variantes em background usando CSV"""
    
    if not is_resume:
        logger.info(f"🚀 INICIANDO PROCESSAMENTO DE VARIANTES: {task_id}")
    else:
        logger.info(f"▶️ RETOMANDO PROCESSAMENTO DE VARIANTES: {task_id}")
    
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
                    
                    # PEGAR O TÍTULO DO PRODUTO
                    product_title = current_product.get("title", f"Produto {product_id}")
                    
                    # ATUALIZAR PROGRESSO COM TÍTULO - MANTÉM SEMPRE PREENCHIDO
                    if task_id in tasks_db:
                        tasks_db[task_id]["progress"]["current_product"] = product_title
                        tasks_db[task_id]["updated_at"] = get_brazil_time_str()
                    
                    # Preparar payload de atualização baseado no submitData
                    update_payload = {
                        "product": {
                            "id": int(product_id)
                        }
                    }
                    
                    # ✅ CORREÇÃO: Aplicar mudanças de título de opções E ORDEM DOS VALORES
                    if submit_data.get("titleChanges") or submit_data.get("orderChanges") or submit_data.get("newValues"):
                        options = []
                        for idx, option in enumerate(current_product.get("options", [])):
                            option_name = option["name"]
                            new_name = submit_data.get("titleChanges", {}).get(option_name, option_name)
                            
                            # Aplicar nova ordem se existir
                            current_values = option.get("values", [])
                            
                            # ✅ CORREÇÃO: Processar orderChanges
                            if submit_data.get("orderChanges") and option_name in submit_data["orderChanges"]:
                                # Reorganizar valores conforme a nova ordem
                                order_data = submit_data["orderChanges"][option_name]
                                ordered_values = []
                                for item in order_data:
                                    value_name = item.get("name", "")
                                    if value_name and value_name in current_values:
                                        ordered_values.append(value_name)
                                # Adicionar valores que não estão na ordem (caso existam)
                                for val in current_values:
                                    if val not in ordered_values:
                                        ordered_values.append(val)
                                current_values = ordered_values
                                logger.info(f"🔄 Aplicando nova ordem para opção '{option_name}': {current_values}")
                            
                            # ✅ CORREÇÃO: Adicionar novos valores se existirem
                            if submit_data.get("newValues") and option_name in submit_data["newValues"]:
                                new_values_list = submit_data["newValues"][option_name]
                                for new_value_data in new_values_list:
                                    new_value_name = new_value_data.get("name", "")
                                    if new_value_name and new_value_name not in current_values:
                                        # Adicionar na posição correta baseado na ordem
                                        order_position = new_value_data.get("order", len(current_values))
                                        current_values.insert(order_position, new_value_name)
                                        logger.info(f"➕ Novo valor '{new_value_name}' adicionado à opção '{option_name}' na posição {order_position}")
                            
                            options.append({
                                "id": option.get("id"),
                                "name": new_name,
                                "position": option.get("position", idx + 1),
                                "values": current_values
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
                            
                            # Aplicar mudanças de valores e preços corretamente
                            if submit_data.get("valueChanges"):
                                for option_name, changes in submit_data["valueChanges"].items():
                                    # Verificar cada campo de opção da variante
                                    for option_field in ["option1", "option2", "option3"]:
                                        current_option_value = variant.get(option_field)
                                        
                                        if current_option_value and current_option_value in changes:
                                            change = changes[current_option_value]
                                            
                                            # Atualizar nome do valor se mudou
                                            if "newName" in change:
                                                updated_variant[option_field] = change["newName"]
                                            
                                            # Calcular preço corretamente
                                            if "extraPrice" in change:
                                                new_extra = float(change["extraPrice"])
                                                original_extra = float(change.get("originalExtraPrice", 0))
                                                
                                                # Calcular o preço base (sem o extra original)
                                                current_price = float(variant.get("price", 0))
                                                base_price = current_price - original_extra
                                                
                                                # Aplicar o NOVO extra (não somar, mas substituir)
                                                new_price = base_price + new_extra
                                                updated_variant["price"] = str(new_price)
                                                
                                                # Atualizar compare_at_price se existir
                                                if variant.get("compare_at_price"):
                                                    compare_price = float(variant["compare_at_price"])
                                                    base_compare = compare_price - original_extra
                                                    new_compare = base_compare + new_extra
                                                    updated_variant["compare_at_price"] = str(new_compare)
                                                
                                                logger.info(f"💰 Atualizando preço da variante {variant.get('id')}:")
                                                logger.info(f"   Preço atual: R$ {current_price}")
                                                logger.info(f"   Extra original: R$ {original_extra}")
                                                logger.info(f"   Preço base: R$ {base_price}")
                                                logger.info(f"   Novo extra: R$ {new_extra}")
                                                logger.info(f"   Novo preço: R$ {new_price}")
                            
                            variants.append(updated_variant)
                        
                        # ✅ CORREÇÃO: Adicionar novas variantes se houver novos valores
                        if submit_data.get("newValues"):
                            logger.info(f"🆕 Processando criação de novas variantes...")
                            
                            # Para cada opção com novos valores
                            for option_name, new_values_list in submit_data["newValues"].items():
                                # Encontrar o índice da opção
                                option_index = None
                                for idx, opt in enumerate(current_product.get("options", [])):
                                    if opt["name"] == option_name:
                                        option_index = idx
                                        break
                                
                                if option_index is None:
                                    logger.warning(f"⚠️ Opção '{option_name}' não encontrada no produto")
                                    continue
                                
                                option_field = f"option{option_index + 1}"
                                
                                # Para cada novo valor
                                for new_value_data in new_values_list:
                                    new_value_name = new_value_data.get("name", "")
                                    extra_price = float(new_value_data.get("extraPrice", 0))
                                    
                                    if not new_value_name:
                                        continue
                                    
                                    logger.info(f"  Criando variantes para novo valor '{new_value_name}' com preço extra R$ {extra_price}")
                                    
                                    # Encontrar todas as combinações existentes das outras opções
                                    existing_combinations = set()
                                    for variant in variants:
                                        combo = []
                                        for i in range(3):
                                            if i != option_index:
                                                combo.append(variant.get(f"option{i+1}"))
                                        existing_combinations.add(tuple(combo))
                                    
                                    # Criar uma nova variante para cada combinação
                                    for combo in existing_combinations:
                                        # Montar a nova variante
                                        new_variant = {
                                            "option1": None,
                                            "option2": None,
                                            "option3": None
                                        }
                                        
                                        # Preencher o novo valor na posição correta
                                        new_variant[option_field] = new_value_name
                                        
                                        # Preencher os outros valores da combinação
                                        combo_index = 0
                                        for i in range(3):
                                            if i != option_index:
                                                new_variant[f"option{i+1}"] = combo[combo_index] if combo_index < len(combo) else None
                                                combo_index += 1
                                        
                                        # Verificar se esta variante já existe
                                        variant_exists = False
                                        for existing_variant in variants:
                                            if (existing_variant.get("option1") == new_variant["option1"] and
                                                existing_variant.get("option2") == new_variant["option2"] and
                                                existing_variant.get("option3") == new_variant["option3"]):
                                                variant_exists = True
                                                break
                                        
                                        if not variant_exists:
                                            # Usar a primeira variante como base para outros campos
                                            base_variant = current_product.get("variants", [{}])[0]
                                            base_price = float(base_variant.get("price", 0))
                                            
                                            # Criar a nova variante completa
                                            complete_variant = {
                                                "option1": new_variant["option1"],
                                                "option2": new_variant["option2"],
                                                "option3": new_variant["option3"],
                                                "price": str(base_price + extra_price),
                                                "sku": f"{base_variant.get('sku', '')}-{new_value_name.replace(' ', '-').lower()}",
                                                "inventory_quantity": 0,
                                                "inventory_management": "shopify",
                                                "inventory_policy": "continue",
                                                "fulfillment_service": "manual",
                                                "requires_shipping": base_variant.get("requires_shipping", True),
                                                "taxable": base_variant.get("taxable", True),
                                                "barcode": base_variant.get("barcode"),
                                                "grams": base_variant.get("grams", 0),
                                                "weight": base_variant.get("weight", 0),
                                                "weight_unit": base_variant.get("weight_unit", "kg")
                                            }
                                            
                                            # Adicionar compare_at_price se existir
                                            if base_variant.get("compare_at_price"):
                                                base_compare = float(base_variant["compare_at_price"])
                                                complete_variant["compare_at_price"] = str(base_compare + extra_price)
                                            
                                            variants.append(complete_variant)
                                            logger.info(f"    ✅ Nova variante criada: {new_variant['option1']} | {new_variant['option2']} | {new_variant['option3']}")
                        
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
                            "product_title": product_title,
                            "status": "success",
                            "message": "Variantes atualizadas com sucesso"
                        }
                        logger.info(f"✅ Produto '{product_title}' atualizado")
                    else:
                        failed += 1
                        error_text = await update_response.text()
                        result = {
                            "product_id": product_id,
                            "product_title": product_title,
                            "status": "failed",
                            "message": f"Erro: {error_text}"
                        }
                        logger.error(f"❌ Erro no produto '{product_title}': {error_text}")
                
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
            
            # IMPORTANTE: NÃO LIMPAR current_product AQUI - MANTÉM ATÉ O PRÓXIMO
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "percentage": percentage,
                    "current_product": product_title if i < len(product_ids)-1 else None  # SÓ LIMPA NO FINAL
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
        tasks_db[task_id]["progress"]["current_product"] = None  # LIMPAR APENAS NO FINAL
        
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
            
            # PEGAR O TÍTULO DO PRODUTO
            product_title = current_product.get("title", f"Produto {product_id}")
            
            # ATUALIZAR STATUS DA TAREFA COM TÍTULO
            if task_id in tasks_db:
                tasks_db[task_id]["progress"]["current_product"] = product_title
                tasks_db[task_id]["updated_at"] = get_brazil_time_str()
            
            # Preparar payload de atualização
            update_payload = {
                "product": {
                    "id": int(product_id),
                    "options": [],
                    "variants": []
                }
            }
            
            # ✅ CORREÇÃO: Aplicar mudanças de título, ordem e novos valores nas opções
            options = []
            for idx, option in enumerate(current_product.get("options", [])):
                option_name = option["name"]
                new_name = submit_data.get("titleChanges", {}).get(option_name, option_name)
                
                # Aplicar nova ordem se existir
                current_values = option.get("values", [])
                
                # Processar orderChanges
                if submit_data.get("orderChanges") and option_name in submit_data["orderChanges"]:
                    order_data = submit_data["orderChanges"][option_name]
                    ordered_values = []
                    for item in order_data:
                        value_name = item.get("name", "")
                        if value_name and value_name in current_values:
                            ordered_values.append(value_name)
                    for val in current_values:
                        if val not in ordered_values:
                            ordered_values.append(val)
                    current_values = ordered_values
                    logger.info(f"🔄 Aplicando nova ordem para opção '{option_name}'")
                
                # Adicionar novos valores se existirem
                if submit_data.get("newValues") and option_name in submit_data["newValues"]:
                    new_values_list = submit_data["newValues"][option_name]
                    for new_value_data in new_values_list:
                        new_value_name = new_value_data.get("name", "")
                        if new_value_name and new_value_name not in current_values:
                            order_position = new_value_data.get("order", len(current_values))
                            current_values.insert(order_position, new_value_name)
                            logger.info(f"➕ Novo valor '{new_value_name}' adicionado")
                
                options.append({
                    "id": option.get("id"),
                    "name": new_name,
                    "position": option.get("position", idx + 1),
                    "values": current_values
                })
            
            update_payload["product"]["options"] = options
            
            # Aplicar mudanças nas variantes
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
                
                # Aplicar mudanças de valores e preços
                if submit_data.get("valueChanges"):
                    for option_name, changes in submit_data["valueChanges"].items():
                        for option_field in ["option1", "option2", "option3"]:
                            if variant.get(option_field) in changes:
                                change = changes[variant[option_field]]
                                updated_variant[option_field] = change.get("newName", variant[option_field])
                                
                                # Ajustar preço se houver mudança
                                if "extraPrice" in change:
                                    new_extra = float(change["extraPrice"])
                                    original_extra = float(change.get("originalExtraPrice", 0))
                                    current_price = float(variant.get("price", 0))
                                    
                                    # Calcular o preço base removendo o extra original
                                    base_price = current_price - original_extra
                                    
                                    # Aplicar o NOVO extra (substituir, não somar)
                                    updated_variant["price"] = str(base_price + new_extra)
                                    
                                    # Atualizar compare_at_price se existir
                                    if variant.get("compare_at_price"):
                                        compare_price = float(variant["compare_at_price"])
                                        base_compare = compare_price - original_extra
                                        updated_variant["compare_at_price"] = str(base_compare + new_extra)
                                    
                                    logger.info(f"💰 Preço corrigido: Base R$ {base_price} + Extra R$ {new_extra} = R$ {base_price + new_extra}")
                
                variants.append(updated_variant)
            
            # ✅ CORREÇÃO: Adicionar novas variantes se houver novos valores
            if submit_data.get("newValues"):
                logger.info(f"🆕 Criando novas variantes...")
                
                for option_name, new_values_list in submit_data["newValues"].items():
                    # Encontrar índice da opção
                    option_index = None
                    for idx, opt in enumerate(options):
                        if opt["name"] == option_name or (option_name in submit_data.get("titleChanges", {}) and opt["name"] == submit_data["titleChanges"][option_name]):
                            option_index = idx
                            break
                    
                    if option_index is None:
                        continue
                    
                    option_field = f"option{option_index + 1}"
                    
                    for new_value_data in new_values_list:
                        new_value_name = new_value_data.get("name", "")
                        extra_price = float(new_value_data.get("extraPrice", 0))
                        
                        if not new_value_name:
                            continue
                        
                        # Criar combinações com outros valores
                        existing_combinations = set()
                        for variant in variants:
                            combo = []
                            for i in range(3):
                                if i != option_index:
                                    combo.append(variant.get(f"option{i+1}"))
                            existing_combinations.add(tuple(combo))
                        
                        for combo in existing_combinations:
                            new_variant_options = {
                                "option1": None,
                                "option2": None,
                                "option3": None
                            }
                            
                            new_variant_options[option_field] = new_value_name
                            
                            combo_index = 0
                            for i in range(3):
                                if i != option_index:
                                    new_variant_options[f"option{i+1}"] = combo[combo_index] if combo_index < len(combo) else None
                                    combo_index += 1
                            
                            # Verificar se já existe
                            variant_exists = False
                            for existing_variant in variants:
                                if (existing_variant.get("option1") == new_variant_options["option1"] and
                                    existing_variant.get("option2") == new_variant_options["option2"] and
                                    existing_variant.get("option3") == new_variant_options["option3"]):
                                    variant_exists = True
                                    break
                            
                            if not variant_exists:
                                base_variant = current_product.get("variants", [{}])[0]
                                base_price = float(base_variant.get("price", 0))
                                
                                complete_variant = {
                                    "option1": new_variant_options["option1"],
                                    "option2": new_variant_options["option2"],
                                    "option3": new_variant_options["option3"],
                                    "price": str(base_price + extra_price),
                                    "sku": f"{base_variant.get('sku', '')}-{new_value_name.replace(' ', '-').lower()}",
                                    "inventory_quantity": 0,
                                    "inventory_management": "shopify",
                                    "inventory_policy": "continue",
                                    "fulfillment_service": "manual",
                                    "requires_shipping": base_variant.get("requires_shipping", True),
                                    "taxable": base_variant.get("taxable", True),
                                    "barcode": base_variant.get("barcode"),
                                    "grams": base_variant.get("grams", 0),
                                    "weight": base_variant.get("weight", 0),
                                    "weight_unit": base_variant.get("weight_unit", "kg")
                                }
                                
                                if base_variant.get("compare_at_price"):
                                    base_compare = float(base_variant["compare_at_price"])
                                    complete_variant["compare_at_price"] = str(base_compare + extra_price)
                                
                                variants.append(complete_variant)
                                logger.info(f"✅ Nova variante criada")
            
            update_payload["product"]["variants"] = variants
            
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
                logger.info(f"✅ Produto '{product_title}' atualizado com sucesso")
            else:
                error_text = await update_response.text()
                if task_id in tasks_db:
                    tasks_db[task_id]["status"] = "failed"
                    tasks_db[task_id]["error_message"] = error_text
                    tasks_db[task_id]["completed_at"] = get_brazil_time_str()
                    tasks_db[task_id]["progress"]["processed"] = 1
                    tasks_db[task_id]["progress"]["failed"] = 1
                logger.error(f"❌ Erro ao atualizar produto '{product_title}': {error_text}")
    
    except Exception as e:
        logger.error(f"❌ Exceção no processamento de variantes: {str(e)}")
        if task_id in tasks_db:
            tasks_db[task_id]["status"] = "failed"
            tasks_db[task_id]["error_message"] = str(e)
            tasks_db[task_id]["completed_at"] = get_brazil_time_str()
            tasks_db[task_id]["progress"]["processed"] = 1
            tasks_db[task_id]["progress"]["failed"] = 1

# ==================== ATUALIZAR PRODUTOS DO SHOPIFY ====================

@app.post("/api/products/refresh")
async def refresh_products_from_shopify(data: Dict[str, Any]):
    """Buscar produtos atualizados diretamente do Shopify"""
    
    store_name = data.get("storeName", "")
    access_token = data.get("accessToken", "")
    
    if not store_name or not access_token:
        raise HTTPException(status_code=400, detail="storeName e accessToken são obrigatórios")
    
    logger.info(f"🔄 Buscando produtos atualizados do Shopify para {store_name}")
    
    # Limpar nome da loja
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    try:
        all_products = []
        
        # Buscar primeira página
        url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products.json?limit=250"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Primeira requisição
            response = await client.get(url, headers=headers)
            
            if response.status_code != 200:
                error_text = await response.text()
                logger.error(f"❌ Erro ao buscar produtos: {error_text}")
                raise HTTPException(status_code=response.status_code, detail=f"Erro do Shopify: {error_text}")
            
            data = response.json()
            products = data.get("products", [])
            all_products.extend(products)
            
            logger.info(f"📦 Primeira página: {len(products)} produtos")
            
            # Verificar se há mais páginas através do header Link
            link_header = response.headers.get("link", "")
            
            # Continuar buscando páginas enquanto houver
            page_count = 1
            while link_header and 'rel="next"' in link_header:
                # Extrair URL da próxima página
                parts = link_header.split(",")
                next_url = None
                
                for part in parts:
                    if 'rel="next"' in part:
                        # Extrair URL entre < e >
                        start = part.find("<") + 1
                        end = part.find(">")
                        if start > 0 and end > start:
                            next_url = part[start:end]
                            break
                
                if not next_url:
                    break
                
                # Buscar próxima página
                response = await client.get(next_url, headers=headers)
                
                if response.status_code != 200:
                    logger.warning(f"⚠️ Erro ao buscar página {page_count + 1}, parando paginação")
                    break
                
                data = response.json()
                products = data.get("products", [])
                all_products.extend(products)
                
                page_count += 1
                logger.info(f"📦 Página {page_count}: {len(products)} produtos (Total: {len(all_products)})")
                
                # Atualizar link header
                link_header = response.headers.get("link", "")
                
                # Rate limiting
                await asyncio.sleep(0.5)
        
        # Buscar informações adicionais se necessário (variants completas)
        logger.info(f"✅ Total de {len(all_products)} produtos carregados do Shopify")
        
        # Enriquecer com dados de variants se necessário
        for product in all_products:
            # Garantir que variants estão presentes
            if "variants" not in product or not product["variants"]:
                product["variants"] = []
            
            # Garantir que options estão presentes
            if "options" not in product or not product["options"]:
                product["options"] = []
        
        return all_products
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar produtos do Shopify: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

# ==================== ATUALIZAÇÃO DE IMAGENS ====================
@app.post("/api/products/refresh-images")
async def refresh_product_images(data: Dict[str, Any]):
    """Buscar apenas as imagens atualizadas de produtos específicos"""
    
    product_ids = data.get("productIds", [])
    store_name = data.get("storeName", "")
    access_token = data.get("accessToken", "")
    
    if not product_ids or not store_name or not access_token:
        raise HTTPException(status_code=400, detail="Dados incompletos")
    
    logger.info(f"🔄 Buscando imagens atualizadas para {len(product_ids)} produtos")
    
    clean_store = store_name.replace('.myshopify.com', '').strip()
    api_version = '2024-04'
    
    try:
        updated_products = []
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for product_id in product_ids[:50]:  # Limitar a 50 produtos por vez
                try:
                    url = f"https://{clean_store}.myshopify.com/admin/api/{api_version}/products/{product_id}.json"
                    headers = {
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    }
                    
                    response = await client.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        product_data = response.json().get("product", {})
                        
                        # Extrair apenas dados essenciais de imagens
                        simplified_product = {
                            "id": product_data.get("id"),
                            "images": product_data.get("images", []),
                            "featured_image": product_data.get("image")
                        }
                        
                        updated_products.append(simplified_product)
                    
                    await asyncio.sleep(0.1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Erro ao buscar produto {product_id}: {e}")
                    continue
        
        logger.info(f"✅ {len(updated_products)} produtos com imagens atualizadas")
        
        return updated_products
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar imagens: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== AGENDAMENTO DE TAREFAS (CORRIGIDOS) ====================

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
    
    # NOVO: Processar notificações se configuradas
    notification_scheduled_for = None
    if data.get("notifications"):
        notifications = data["notifications"]
        if notifications.get("before_execution"):
            notification_time_minutes = notifications.get("notification_time", 30)
            
            # Calcular horário da notificação
            notification_datetime = scheduled_time_naive - timedelta(minutes=notification_time_minutes)
            notification_scheduled_for = notification_datetime.isoformat()
            
            logger.info(f"📱 Notificação configurada para: {notification_datetime}")
            logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
    
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
            "notification_scheduled_for": notification_scheduled_for,  # NOVO
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")  # NOVO: Salvar notificações
            },
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
            "notification_scheduled_for": notification_scheduled_for,  # NOVO
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")  # NOVO: Salvar notificações
            },
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

# ==================== AGENDAMENTO DE VARIANTES (CORRIGIDO) ====================

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
    
    # NOVO: Processar notificações se configuradas
    notification_scheduled_for = None
    if data.get("notifications"):
        notifications = data["notifications"]
        if notifications.get("before_execution"):
            notification_time_minutes = notifications.get("notification_time", 30)
            
            # Calcular horário da notificação
            notification_datetime = scheduled_time_naive - timedelta(minutes=notification_time_minutes)
            notification_scheduled_for = notification_datetime.isoformat()
            
            logger.info(f"📱 Notificação configurada para: {notification_datetime}")
            logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
    
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
            "notification_scheduled_for": notification_scheduled_for,
            "notifications": data.get("notifications"),  # ✅ CORREÇÃO: Adicionar notificações
            "started_at": get_brazil_time_str(),
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")
            },
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
            "notification_scheduled_for": notification_scheduled_for,
            "notifications": data.get("notifications"),  # ✅ CORREÇÃO: Adicionar notificações
            "priority": data.get("priority", "medium"),
            "description": data.get("description", ""),
            "config": {
                **data.get("config", {}),
                "notifications": data.get("notifications")
            },
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
    """Retomar uma tarefa pausada - VERSÃO MELHORADA COM SUPORTE A VARIANTES E RENOMEAÇÃO"""
    
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
    
    # Verificar o tipo de tarefa
    task_type = task.get("task_type", "bulk_edit")
    config = task.get("config", {})
    
    logger.info(f"▶️ Retomando tarefa {task_id} (tipo: {task_type})")
    
    if task_type == "variant_management":
        # RETOMAR VARIANTES
        all_product_ids = config.get("productIds", [])
        processed_count = task.get("progress", {}).get("processed", 0)
        remaining_products = all_product_ids[processed_count:]
        
        logger.info(f"   Total de produtos: {len(all_product_ids)}")
        logger.info(f"   Já processados: {processed_count}")
        logger.info(f"   Restantes: {len(remaining_products)}")
        
        if len(remaining_products) > 0:
            # Processar variantes restantes
            background_tasks.add_task(
                process_variants_background,
                task_id,
                config.get("csvContent", ""),
                remaining_products,  # Apenas produtos restantes
                config.get("submitData", {}),
                config.get("storeName", ""),
                config.get("accessToken", ""),
                is_resume=True  # Adicionar flag de retomada
            )
            
            logger.info(f"✅ Tarefa de variantes {task_id} retomada com {len(remaining_products)} produtos")
            
            return {
                "success": True,
                "message": f"Tarefa de variantes retomada com sucesso",
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
    elif task_type == "alt_text":
        # RETOMAR ALT-TEXT
        all_images = config.get("csvData", [])
        processed_count = task.get("progress", {}).get("processed", 0)
        remaining_images = all_images[processed_count:]
        
        logger.info(f"   Total de imagens: {len(all_images)}")
        logger.info(f"   Já processadas: {processed_count}")
        logger.info(f"   Restantes: {len(remaining_images)}")
        
        if len(remaining_images) > 0:
            background_tasks.add_task(
                process_alt_text_background,
                task_id,
                remaining_images,
                config.get("storeName", ""),
                config.get("accessToken", ""),
                is_resume=True
            )
            
            logger.info(f"✅ Tarefa de alt-text {task_id} retomada com {len(remaining_images)} imagens")
            
            return {
                "success": True,
                "message": f"Tarefa de alt-text retomada com sucesso",
                "task": task,
                "remaining": len(remaining_images)
            }
        else:
            task["status"] = "completed"
            task["completed_at"] = get_brazil_time_str()
            
            return {
                "success": True,
                "message": "Tarefa já estava completa",
                "task": task
            }
    elif task_type == "rename_images":
        # RETOMAR RENOMEAÇÃO DE IMAGENS
        all_images = config.get("images", [])
        processed_count = task.get("progress", {}).get("processed", 0)
        remaining_images = all_images[processed_count:]
        
        logger.info(f"📸 Retomando renomeação de imagens:")
        logger.info(f"   Total de imagens: {len(all_images)}")
        logger.info(f"   Já processadas: {processed_count}")
        logger.info(f"   Restantes: {len(remaining_images)}")
        
        if len(remaining_images) > 0:
            # Retomar processamento das imagens restantes
            background_tasks.add_task(
                process_rename_images_background,
                task_id,
                config.get("template", ""),
                remaining_images,
                config.get("storeName", ""),
                config.get("accessToken", ""),
                is_resume=True
            )
            
            logger.info(f"✅ Tarefa de renomeação {task_id} retomada com {len(remaining_images)} imagens")
            
            return {
                "success": True,
                "message": f"Tarefa de renomeação retomada com sucesso",
                "task": task,
                "remaining": len(remaining_images),
                "progress": task.get("progress")
            }
        else:
            # Se não há imagens restantes, marcar como completa
            task["status"] = "completed"
            task["completed_at"] = get_brazil_time_str()
            
            return {
                "success": True,
                "message": "Tarefa já estava completa",
                "task": task
            }
    elif task_type == "image_optimization":
        # RETOMAR OTIMIZAÇÃO DE IMAGENS
        all_images = config.get("images", [])
        processed_count = task.get("progress", {}).get("processed", 0)
        remaining_count = len(all_images) - processed_count
        
        # PEGAR targetHeight DO CONFIG!
        target_height = config.get("targetHeight")
        if not target_height:
            logger.error(f"❌ targetHeight não encontrado no config da tarefa {task_id}")
            return {
                "success": False,
                "message": "targetHeight não configurado na tarefa"
            }
        
        logger.info(f"🖼️ Retomando otimização de imagens:")
        logger.info(f"   Total de imagens: {len(all_images)}")
        logger.info(f"   Já processadas: {processed_count}")
        logger.info(f"   Restantes: {remaining_count}")
        logger.info(f"   Altura alvo: {target_height}px")
        
        if remaining_count > 0:
            # IMPORTANTE: Passar TODAS as imagens, não apenas as restantes
            background_tasks.add_task(
                process_image_optimization_background,
                task_id,
                all_images,  # Passar TODAS as imagens
                target_height,
                config.get("storeName", ""),
                config.get("accessToken", ""),
                is_resume=True  # Flag para indicar retomada
            )
            
            logger.info(f"✅ Tarefa de otimização {task_id} retomada com {remaining_count} imagens restantes")
            
            return {
                "success": True,
                "message": f"Tarefa de otimização retomada com sucesso",
                "task": task,
                "remaining": remaining_count,
                "progress": task.get("progress")
            }
        else:
            task["status"] = "completed"
            task["completed_at"] = get_brazil_time_str()
            
            return {
                "success": True,
                "message": "Tarefa já estava completa",
                "task": task
            }
    else:
        # RETOMAR BULK EDIT NORMAL
        all_product_ids = config.get("productIds", [])
        processed_count = task.get("progress", {}).get("processed", 0)
        remaining_products = all_product_ids[processed_count:]
        
        logger.info(f"   Total de produtos: {len(all_product_ids)}")
        logger.info(f"   Já processados: {processed_count}")
        logger.info(f"   Restantes: {len(remaining_products)}")
        
        if len(remaining_products) > 0:
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
    """Endpoint simples /tasks para compatibilidade - OTIMIZADO PARA TASKSRUNNING"""
    # RETORNAR APENAS TAREFAS ATIVAS E RECENTES!
    active_tasks = []
    recent_completed = []
    
    now = datetime.now()
    
    for task_id, task in tasks_db.items():
        status = task.get("status")
        
        # Sempre incluir tarefas ativas
        if status in ["processing", "running", "paused", "scheduled"]:
            active_tasks.append(task)
        # Incluir tarefas completadas das últimas 2 horas apenas
        elif status in ["completed", "completed_with_errors", "failed", "cancelled"]:
            completed_at = task.get("completed_at") or task.get("updated_at")
            if completed_at:
                try:
                    completed_time = datetime.fromisoformat(completed_at.replace('Z', ''))
                    # Só incluir se foi completada nas últimas 2 horas
                    if (now - completed_time).total_seconds() < 7200:  # 2 horas
                        # Criar versão simplificada da tarefa completada
                        simplified_task = {
                            "id": task["id"],
                            "name": task.get("name"),
                            "status": task["status"],
                            "task_type": task.get("task_type", "bulk_edit"),
                            "progress": task.get("progress", {}),
                            "started_at": task.get("started_at"),
                            "completed_at": task.get("completed_at"),
                            "updated_at": task.get("updated_at"),
                            # NÃO incluir config completo ou results grandes
                            "config": {
                                "itemCount": task.get("config", {}).get("itemCount", 0)
                            },
                            # Limitar results a 5 últimos
                            "results": task.get("results", [])[-5:] if "results" in task else []
                        }
                        recent_completed.append(simplified_task)
                except:
                    pass
    
    # Combinar tarefas ativas e recentes
    tasks_list = active_tasks + recent_completed
    
    # Ordenar por updated_at
    tasks_list.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    logger.info(f"📋 Retornando {len(active_tasks)} tarefas ativas e {len(recent_completed)} recentes")
    
    return {
        "success": True,
        "tasks": tasks_list,
        "total": len(tasks_list)
    }

@app.get("/api/tasks/all")
async def get_all_tasks():
    """Retornar TODAS as tarefas com estatísticas - OTIMIZADO"""
    all_tasks = []
    stats = {
        "scheduled": 0,
        "processing": 0,
        "paused": 0,
        "completed": 0,
        "completed_with_errors": 0,
        "failed": 0,
        "cancelled": 0
    }
    
    for task_id, task in tasks_db.items():
        status = task.get("status")
        
        # Atualizar estatísticas
        if status == "scheduled":
            stats["scheduled"] += 1
        elif status in ["processing", "running"]:
            stats["processing"] += 1
        elif status == "paused":
            stats["paused"] += 1
        elif status == "completed":
            stats["completed"] += 1
        elif status == "completed_with_errors":
            stats["completed_with_errors"] += 1
        elif status == "failed":
            stats["failed"] += 1
        elif status == "cancelled":
            stats["cancelled"] += 1
        
        # Para tarefas completadas, criar versão simplificada
        if status in ["completed", "completed_with_errors", "failed", "cancelled"]:
            simplified_task = {
                "id": task["id"],
                "name": task.get("name"),
                "status": status,
                "task_type": task.get("task_type", "bulk_edit"),
                "priority": task.get("priority", "medium"),
                "progress": {
                    "processed": task.get("progress", {}).get("processed", 0),
                    "total": task.get("progress", {}).get("total", 0),
                    "successful": task.get("progress", {}).get("successful", 0),
                    "failed": task.get("progress", {}).get("failed", 0),
                    "percentage": task.get("progress", {}).get("percentage", 0)
                },
                "started_at": task.get("started_at"),
                "completed_at": task.get("completed_at"),
                "updated_at": task.get("updated_at"),
                "created_at": task.get("created_at"),
                # Dados mínimos de config
                "config": {
                    "itemCount": task.get("config", {}).get("itemCount", 0),
                    "storeName": task.get("config", {}).get("storeName", "")
                },
                # Sem results completos
                "results_count": len(task.get("results", []))
            }
            all_tasks.append(simplified_task)
        else:
            # Tarefas ativas podem ter mais detalhes
            all_tasks.append(task)
    
    # Ordenar por updated_at mais recente
    all_tasks.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    # Limitar a 100 tarefas mais recentes para evitar sobrecarga
    if len(all_tasks) > 100:
        all_tasks = all_tasks[:100]
        logger.info(f"⚠️ Limitando resposta a 100 tarefas mais recentes (total no DB: {len(tasks_db)})")
    
    return {
        "success": True,
        "total": len(all_tasks),
        "total_in_db": len(tasks_db),
        "tasks": all_tasks,
        "stats": stats
    }

@app.get("/api/tasks/scheduled")
async def get_scheduled_tasks():
    """Retornar APENAS tarefas agendadas - JÁ OTIMIZADO"""
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
    """Retornar tarefas em execução e pausadas - OTIMIZADO"""
    active_tasks = []
    
    for task_id, task in tasks_db.items():
        if task.get("status") in ["processing", "running", "paused"]:
            # Para tarefas de renomeação com muitas imagens, simplificar
            if task.get("task_type") == "rename_images" and len(task.get("config", {}).get("images", [])) > 50:
                # Criar versão simplificada
                simplified_task = dict(task)  # Cópia do task
                # Reduzir config
                simplified_task["config"] = {
                    "template": task.get("config", {}).get("template"),
                    "itemCount": task.get("config", {}).get("itemCount", 0),
                    "storeName": task.get("config", {}).get("storeName"),
                    "accessToken": task.get("config", {}).get("accessToken"),
                    # NÃO incluir array completo de images
                }
                # Limitar results
                if "results" in simplified_task:
                    simplified_task["results"] = simplified_task["results"][-10:]
                active_tasks.append(simplified_task)
            else:
                active_tasks.append(task)
    
    # Ordenar por progresso
    active_tasks.sort(key=lambda x: x.get("progress", {}).get("percentage", 0))
    
    logger.info(f"🏃 Retornando {len(active_tasks)} tarefas ativas")
    
    return {
        "success": True,
        "total": len(active_tasks),
        "tasks": active_tasks
    }

# ==================== LIMPEZA AUTOMÁTICA DE MEMÓRIA ====================

async def cleanup_old_tasks():
    """Limpar tarefas antigas da memória para evitar acúmulo"""
    while True:
        try:
            await asyncio.sleep(300)  # Aguardar 5 minutos
            
            now = datetime.now()
            tasks_to_remove = []
            tasks_to_simplify = []
            
            for task_id, task in tasks_db.items():
                status = task.get("status")
                
                # Remover tarefas completadas há mais de 24 horas
                if status in ["completed", "failed", "cancelled", "completed_with_errors"]:
                    completed_at = task.get("completed_at") or task.get("updated_at")
                    if completed_at:
                        try:
                            completed_time = datetime.fromisoformat(completed_at.replace('Z', ''))
                            hours_passed = (now - completed_time).total_seconds() / 3600
                            
                            if hours_passed > 24:  # Mais de 24 horas
                                tasks_to_remove.append(task_id)
                            elif hours_passed > 2:  # Entre 2 e 24 horas - simplificar
                                tasks_to_simplify.append(task_id)
                        except:
                            pass
            
            # Remover tarefas muito antigas
            for task_id in tasks_to_remove:
                del tasks_db[task_id]
                logger.info(f"🗑️ Tarefa antiga removida da memória: {task_id}")
            
            # Simplificar tarefas completadas recentes (liberar memória mas manter registro)
            for task_id in tasks_to_simplify:
                if task_id in tasks_db:
                    task = tasks_db[task_id]
                    # Manter apenas informações essenciais
                    tasks_db[task_id] = {
                        "id": task["id"],
                        "name": task.get("name"),
                        "status": task["status"],
                        "task_type": task.get("task_type"),
                        "progress": task.get("progress"),
                        "started_at": task.get("started_at"),
                        "completed_at": task.get("completed_at"),
                        "updated_at": task.get("updated_at"),
                        "config": {
                            "itemCount": task.get("config", {}).get("itemCount", 0)
                        },
                        "results": []  # Limpar results
                    }
            
            if tasks_to_remove or tasks_to_simplify:
                logger.info(f"🧹 Limpeza: {len(tasks_to_remove)} removidas, {len(tasks_to_simplify)} simplificadas")
                logger.info(f"📊 Total de tarefas na memória: {len(tasks_db)}")
            
        except Exception as e:
            logger.error(f"❌ Erro na limpeza automática: {e}")

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
    
    # IMPORTANTE: Se atualizou o scheduled_for
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
        
        # Atualizar o scheduled_for_local
        task["scheduled_for_local"] = scheduled_time.isoformat()
        
        # NOVO: Recalcular notificações se configuradas
        if task.get("config", {}).get("notifications"):
            notifications = task["config"]["notifications"]
            if notifications.get("before_execution"):
                notification_time_minutes = notifications.get("notification_time", 30)
                
                # Calcular o novo horário da notificação
                notification_datetime = scheduled_time - timedelta(minutes=notification_time_minutes)
                
                # Armazenar o horário da notificação
                task["notification_scheduled_for"] = notification_datetime.isoformat()
                
                # Também atualizar no config para persistência
                task["config"]["notifications"]["scheduled_at"] = notification_datetime.isoformat()
                
                logger.info(f"📱 Notificação reagendada para: {notification_datetime}")
                logger.info(f"   ({notification_time_minutes} minutos antes da execução)")
                
                # Se a notificação já passou mas a tarefa ainda não, desabilitar notificação prévia
                now = datetime.now()
                if notification_datetime <= now < scheduled_time:
                    logger.warning(f"⚠️ Horário da notificação já passou, notificação prévia desabilitada")
                    task["config"]["notifications"]["before_execution_sent"] = True
        
        now = datetime.now()
        
        # Se o novo horário já passou, executar imediatamente
        if scheduled_time <= now:
            logger.info(f"📝 Tarefa {task_id} atualizada para horário passado, executando imediatamente!")
            
            # Mudar status e processar
            task["status"] = "processing"
            task["started_at"] = get_brazil_time_str()
            
            config = task.get("config", {})
            
            # Determinar o tipo de tarefa e processar adequadamente
            task_type = task.get("task_type", "bulk_edit")
            
            if task_type == "alt_text":
                # Processar alt-text
                background_tasks.add_task(
                    process_alt_text_background,
                    task_id,
                    config.get("csvData", []),
                    config.get("storeName", ""),
                    config.get("accessToken", "")
                )
            elif task_type == "variant_management":
                # Processar variantes
                if config.get("csvContent"):
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
                    background_tasks.add_task(
                        process_single_product_variants,
                        task_id,
                        config.get("productId"),
                        config.get("submitData", {}),
                        config.get("storeName", ""),
                        config.get("accessToken", "")
                    )
            else:
                # Processar bulk edit normal
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
            # Tarefa ainda está no futuro
            logger.info(f"📅 Tarefa {task_id} reagendada para {scheduled_time}")
            
            # Calcular tempo restante
            time_remaining = (scheduled_time - now).total_seconds()
            hours = int(time_remaining // 3600)
            minutes = int((time_remaining % 3600) // 60)
            
            if hours > 0:
                logger.info(f"⏱️ Será executada em {hours}h {minutes}min")
            else:
                logger.info(f"⏱️ Será executada em {minutes} minutos")
    else:
        logger.info(f"📝 Tarefa {task_id} atualizada")
    
    return {
        "success": True,
        "task": task,
        "message": "Tarefa atualizada com sucesso"
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
                
                # PEGAR O TÍTULO DO PRODUTO
                product_title = current_product.get("title", "Sem título")
                
                # ATUALIZAR PROGRESSO COM TÍTULO ANTES DE PROCESSAR
                if task_id in tasks_db:
                    tasks_db[task_id]["progress"]["current_product"] = product_title
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
                        "product_title": product_title,
                        "status": "success",
                        "message": "Produto atualizado com sucesso"
                    }
                    logger.info(f"✅ Produto '{product_title}' atualizado")
                else:
                    failed += 1
                    error_text = await update_response.text()
                    result = {
                        "product_id": product_id,
                        "product_title": product_title,
                        "status": "failed",
                        "message": f"Erro HTTP {update_response.status_code}: {error_text}"
                    }
                    logger.error(f"❌ Erro no produto '{product_title}': {error_text}")
                    
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
            
            # IMPORTANTE: MANTER current_product PREENCHIDO ATÉ O PRÓXIMO
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "percentage": percentage,
                    "current_product": product_title if i < len(product_ids)-1 else None  # SÓ LIMPA NO FINAL
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
                        elif task.get("task_type") == "alt_text":
                            # Processar alt-text
                            asyncio.create_task(
                                process_alt_text_background(
                                    task_id,
                                    config.get("csvData", []),
                                    config.get("storeName", ""),
                                    config.get("accessToken", "")
                                )
                            )
                        elif task.get("task_type") == "rename_images":
                            # Processar renomeação de imagens
                            logger.info(f"🖼️ Executando tarefa agendada de renomeação: {task_id}")
                            
                            asyncio.create_task(
                                process_rename_images_background(
                                    task_id,
                                    config.get("template", ""),
                                    config.get("images", []),
                                    config.get("storeName", ""),
                                    config.get("accessToken", "")
                                )
                            )
                        elif task.get("task_type") == "image_optimization":
                            # Processar otimização de imagens
                            logger.info(f"🖼️ Executando tarefa agendada de otimização: {task_id}")
                            
                            # PEGAR targetHeight DO CONFIG!
                            target_height = config.get("targetHeight")
                            if not target_height:
                                logger.error(f"❌ targetHeight não encontrado no config da tarefa {task_id}")
                                task["status"] = "failed"
                                task["error"] = "targetHeight não configurado"
                                continue
                            
                            asyncio.create_task(
                                process_image_optimization_background(
                                    task_id,
                                    config.get("images", []),
                                    target_height,  # USAR O targetHeight DO CONFIG
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

# Adicionar ao startup
@app.on_event("startup")
async def startup_event():
    """Iniciar tarefas de background"""
    asyncio.create_task(check_and_execute_scheduled_tasks())
    asyncio.create_task(cleanup_old_tasks())
    asyncio.create_task(cleanup_temp_images())  # ADICIONAR ESTA LINHA
    logger.info("⏰ Verificador de tarefas agendadas iniciado")
    logger.info("🧹 Sistema de limpeza automática de memória iniciado")
    logger.info("🖼️ Sistema de limpeza de imagens temporárias iniciado")  # ADICIONAR ESTA LINHA

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"🚀 Railway Shopify Processor v3.0 iniciado na porta {port}")
    logger.info(f"✅ Sistema completo de gerenciamento de tarefas ativo!")
    logger.info(f"📋 Funcionalidades: Agendar, Processar, Pausar, Retomar, Cancelar")
    uvicorn.run(app, host="0.0.0.0", port=port)