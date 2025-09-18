from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Set
import httpx
import asyncio
import json
from datetime import datetime
import uvicorn
import logging
import base64
import hashlib
import secrets
import re

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Shopify Complete API", version="3.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurações Shopify OAuth
SHOPIFY_CLIENT_ID = '693504497ae272445cf6669125acb675'
SHOPIFY_CLIENT_SECRET = '2a439b637ec139339bcb1de149063eb5'

# Armazenar tarefas e conexões WebSocket
tasks_db = {}
websocket_connections: Dict[str, Set[WebSocket]] = {}

# ==================== MODELOS ====================
class TaskRequest(BaseModel):
    id: str
    name: str
    task_type: str
    config: Dict[str, Any]
    status: Optional[str] = "scheduled"

class TaskUpdate(BaseModel):
    taskId: str
    status: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None

class OAuthRequest(BaseModel):
    code: str
    shop: str

class ImageRenameRequest(BaseModel):
    connection: Dict[str, str]
    productId: str
    imageId: str
    newFilename: str
    position: Optional[int] = 1
    variantIds: Optional[List[int]] = []
    attachment: str
    altText: Optional[str] = None

class FilenameUpdateRequest(BaseModel):
    connection: Dict[str, str]
    imageId: str
    newFilename: str

# ==================== WEBSOCKET MANAGER ====================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, task_id: str):
        await websocket.accept()
        if task_id not in self.active_connections:
            self.active_connections[task_id] = []
        self.active_connections[task_id].append(websocket)
        logger.info(f"✅ WebSocket conectado para tarefa {task_id}")
    
    def disconnect(self, websocket: WebSocket, task_id: str):
        if task_id in self.active_connections:
            if websocket in self.active_connections[task_id]:
                self.active_connections[task_id].remove(websocket)
            if not self.active_connections[task_id]:
                del self.active_connections[task_id]
        logger.info(f"❌ WebSocket desconectado para tarefa {task_id}")
    
    async def broadcast_task_update(self, task_id: str, data: dict):
        """Enviar atualização para todos conectados a uma tarefa"""
        if task_id in self.active_connections:
            message = json.dumps(data)
            disconnected = []
            for connection in self.active_connections[task_id]:
                try:
                    await connection.send_text(message)
                except:
                    disconnected.append(connection)
            # Remover conexões mortas
            for conn in disconnected:
                self.disconnect(conn, task_id)

manager = ConnectionManager()

# ==================== ROTAS PRINCIPAIS ====================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "message": "Railway API - Complete Shopify Solution",
        "version": "3.0.0",
        "railway_enabled": True,
        "websocket_enabled": True,
        "tasks_active": len([t for t in tasks_db.values() if t.get("status") == "running"]),
        "websocket_connections": sum(len(conns) for conns in manager.active_connections.values())
    }

@app.get("/health")
async def health():
    """Health check detalhado"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "tasks": {
            "total": len(tasks_db),
            "running": sum(1 for t in tasks_db.values() if t["status"] == "running"),
            "completed": sum(1 for t in tasks_db.values() if t["status"] == "completed"),
            "failed": sum(1 for t in tasks_db.values() if t["status"] == "failed")
        }
    }

# ==================== SHOPIFY PROXY ====================

@app.get("/proxy")
@app.post("/proxy")
@app.put("/proxy")
@app.delete("/proxy")
async def shopify_proxy(request: Request):
    """Proxy para requisições Shopify"""
    try:
        # Pegar URL do Shopify dos query params
        shopify_url = request.query_params.get('url')
        
        if not shopify_url:
            raise HTTPException(status_code=400, detail="URL parameter is required")
        
        if not ('myshopify.com' in shopify_url or 'shopify.com' in shopify_url):
            raise HTTPException(status_code=400, detail="Invalid Shopify URL")
        
        # Copiar headers relevantes
        proxy_headers = {}
        for header in ['X-Shopify-Access-Token', 'Content-Type', 'Accept']:
            if header in request.headers:
                proxy_headers[header] = request.headers[header]
        
        # Fazer requisição para Shopify
        async with httpx.AsyncClient(timeout=30.0) as client:
            if request.method == "GET":
                response = await client.get(shopify_url, headers=proxy_headers)
            elif request.method == "POST":
                body = await request.body()
                response = await client.post(shopify_url, headers=proxy_headers, content=body)
            elif request.method == "PUT":
                body = await request.body()
                response = await client.put(shopify_url, headers=proxy_headers, content=body)
            elif request.method == "DELETE":
                response = await client.delete(shopify_url, headers=proxy_headers)
            else:
                raise HTTPException(status_code=405, detail="Method not allowed")
        
        # Retornar resposta
        return JSONResponse(
            content=response.json() if response.headers.get('content-type', '').startswith('application/json') else {"text": response.text},
            status_code=response.status_code
        )
        
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== PRODUTOS E COLEÇÕES OTIMIZADOS ====================

@app.get("/api/products/all")
async def get_all_products(
    store_name: str,
    access_token: str = Header(None, alias="X-Shopify-Access-Token")
):
    """Carregar todos os produtos com paginação otimizada"""
    if not access_token:
        raise HTTPException(status_code=401, detail="Access token required")
    
    clean_store = store_name.replace('.myshopify.com', '')
    all_products = []
    cursor = None
    page = 0
    
    logger.info(f"📦 Iniciando carregamento de produtos para {clean_store}")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            page += 1
            query = """
            query($cursor: String) {
                products(first: 100, after: $cursor) {
                    edges {
                        node {
                            id
                            title
                            handle
                            status
                            createdAt
                            updatedAt
                            productType
                            vendor
                            tags
                            bodyHtml
                            publishedAt
                            featuredImage {
                                url
                                altText
                            }
                            images(first: 10) {
                                edges {
                                    node {
                                        id
                                        url
                                        altText
                                    }
                                }
                            }
                            collections(first: 50) {
                                edges {
                                    node {
                                        id
                                        title
                                        handle
                                    }
                                }
                            }
                            variants(first: 100) {
                                edges {
                                    node {
                                        id
                                        title
                                        price
                                        compareAtPrice
                                        inventoryQuantity
                                        sku
                                        barcode
                                        selectedOptions {
                                            name
                                            value
                                        }
                                    }
                                }
                            }
                            options {
                                id
                                name
                                position
                                values
                            }
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """
            
            try:
                response = await client.post(
                    f"https://{clean_store}.myshopify.com/admin/api/2024-10/graphql.json",
                    json={"query": query, "variables": {"cursor": cursor}},
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    }
                )
                
                if response.status_code != 200:
                    logger.error(f"Erro na página {page}: {response.status_code}")
                    break
                
                data = response.json()
                
                if "errors" in data:
                    logger.error(f"Erros GraphQL: {data['errors']}")
                    break
                
                products_data = data.get("data", {}).get("products", {})
                edges = products_data.get("edges", [])
                
                # Processar produtos
                for edge in edges:
                    node = edge["node"]
                    
                    # Processar coleções
                    collection_ids = []
                    for coll_edge in node.get("collections", {}).get("edges", []):
                        coll_id = coll_edge["node"]["id"].split("/")[-1]
                        collection_ids.append(int(coll_id))
                    
                    # Processar imagens
                    images = []
                    for img_edge in node.get("images", {}).get("edges", []):
                        img = img_edge["node"]
                        images.append({
                            "id": int(img["id"].split("/")[-1]) if "/" in img["id"] else img["id"],
                            "url": img["url"],
                            "alt": img.get("altText", "")
                        })
                    
                    # Processar variantes
                    variants = []
                    for var_edge in node.get("variants", {}).get("edges", []):
                        var = var_edge["node"]
                        
                        # Extrair options
                        option1 = option2 = option3 = None
                        for idx, opt in enumerate(var.get("selectedOptions", [])):
                            if idx == 0:
                                option1 = opt["value"]
                            elif idx == 1:
                                option2 = opt["value"]
                            elif idx == 2:
                                option3 = opt["value"]
                        
                        variants.append({
                            "id": int(var["id"].split("/")[-1]),
                            "title": var["title"],
                            "price": var["price"],
                            "compare_at_price": var.get("compareAtPrice"),
                            "inventory_quantity": var.get("inventoryQuantity", 0),
                            "sku": var.get("sku", ""),
                            "barcode": var.get("barcode", ""),
                            "option1": option1,
                            "option2": option2,
                            "option3": option3
                        })
                    
                    # Montar produto
                    product = {
                        "id": int(node["id"].split("/")[-1]),
                        "title": node["title"],
                        "handle": node["handle"],
                        "status": node["status"],
                        "created_at": node["createdAt"],
                        "updated_at": node["updatedAt"],
                        "published_at": node.get("publishedAt"),
                        "product_type": node.get("productType", ""),
                        "vendor": node.get("vendor", ""),
                        "tags": node.get("tags", []),
                        "body_html": node.get("bodyHtml", ""),
                        "featured_image": {
                            "url": node["featuredImage"]["url"],
                            "alt": node["featuredImage"].get("altText", "")
                        } if node.get("featuredImage") else None,
                        "images": images,
                        "collection_ids": collection_ids,
                        "variants": variants,
                        "options": node.get("options", [])
                    }
                    
                    all_products.append(product)
                
                # Verificar se tem próxima página
                page_info = products_data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                
                cursor = page_info.get("endCursor")
                logger.info(f"📦 Página {page} carregada: {len(all_products)} produtos até agora")
                
                # Rate limiting
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Erro carregando produtos página {page}: {e}")
                break
    
    logger.info(f"✅ Total de produtos carregados: {len(all_products)}")
    
    return {
        "success": True,
        "total": len(all_products),
        "products": all_products
    }

@app.get("/api/collections/all")
async def get_all_collections(
    store_name: str,
    access_token: str = Header(None, alias="X-Shopify-Access-Token")
):
    """Carregar todas as coleções com paginação"""
    if not access_token:
        raise HTTPException(status_code=401, detail="Access token required")
    
    clean_store = store_name.replace('.myshopify.com', '')
    all_collections = []
    cursor = None
    page = 0
    
    logger.info(f"📚 Iniciando carregamento de coleções para {clean_store}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            page += 1
            query = """
            query($cursor: String) {
                collections(first: 250, after: $cursor) {
                    edges {
                        node {
                            id
                            title
                            handle
                            description
                            productsCount
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """
            
            try:
                response = await client.post(
                    f"https://{clean_store}.myshopify.com/admin/api/2024-10/graphql.json",
                    json={"query": query, "variables": {"cursor": cursor}},
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    }
                )
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                collections_data = data.get("data", {}).get("collections", {})
                
                for edge in collections_data.get("edges", []):
                    node = edge["node"]
                    all_collections.append({
                        "id": int(node["id"].split("/")[-1]),
                        "title": node["title"],
                        "handle": node["handle"],
                        "description": node.get("description", ""),
                        "products_count": node.get("productsCount", 0)
                    })
                
                page_info = collections_data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                
                cursor = page_info.get("endCursor")
                logger.info(f"📚 Página {page}: {len(all_collections)} coleções")
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Erro carregando coleções: {e}")
                break
    
    logger.info(f"✅ Total de coleções: {len(all_collections)}")
    
    return {
        "success": True,
        "total": len(all_collections),
        "collections": all_collections
    }

@app.post("/api/load-store-data")
async def load_store_data(request: Dict[str, str]):
    """Carregar todos os dados da loja de uma vez (produtos + coleções)"""
    store_name = request.get("store_name", "").replace('.myshopify.com', '')
    access_token = request.get("access_token", "")
    
    if not store_name or not access_token:
        raise HTTPException(status_code=400, detail="Store name and access token required")
    
    try:
        # Carregar em paralelo
        async with httpx.AsyncClient() as client:
            # Carregar produtos e coleções em paralelo
            products_task = get_all_products(store_name, access_token)
            collections_task = get_all_collections(store_name, access_token)
            
            products_result = await products_task
            collections_result = await collections_task
        
        return {
            "success": True,
            "products": products_result["products"],
            "collections": collections_result["collections"],
            "totals": {
                "products": products_result["total"],
                "collections": collections_result["total"]
            }
        }
        
    except Exception as e:
        logger.error(f"Erro carregando dados da loja: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== SHOPIFY OAUTH ====================

@app.post("/api/shopify/oauth/token")
async def shopify_oauth(oauth_data: OAuthRequest):
    """Trocar código OAuth por access token"""
    try:
        shop = oauth_data.shop.lower().strip().replace('.myshopify.com', '')
        
        # Validar nome da loja
        if not shop or not shop.replace('-', '').replace('.', '').isalnum():
            raise HTTPException(status_code=400, detail="Invalid shop name")
        
        # Fazer requisição OAuth
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://{shop}.myshopify.com/admin/oauth/access_token",
                json={
                    "client_id": SHOPIFY_CLIENT_ID,
                    "client_secret": SHOPIFY_CLIENT_SECRET,
                    "code": oauth_data.code
                },
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"OAuth failed: {response.text}"
            )
        
        token_data = response.json()
        
        return {
            "success": True,
            "access_token": token_data.get("access_token"),
            "scope": token_data.get("scope"),
            "shop": shop
        }
        
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== IMAGE OPERATIONS ====================

@app.post("/update-filename")
async def update_filename(data: FilenameUpdateRequest):
    """Atualizar filename de imagem via GraphQL"""
    try:
        store_name = data.connection['store_name'].replace('.myshopify.com', '')
        access_token = data.connection['access_token']
        
        graphql_url = f"https://{store_name}.myshopify.com/admin/api/2024-04/graphql.json"
        
        mutation = """
        mutation fileUpdate($files: [FileUpdateInput!]!) {
            fileUpdate(files: $files) {
                files {
                    ... on MediaImage {
                        id
                        filename
                        alt
                    }
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        variables = {
            "files": [{
                "id": data.imageId,
                "filename": data.newFilename
            }]
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                graphql_url,
                json={"query": mutation, "variables": variables},
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                }
            )
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        
        result = response.json()
        
        if result.get("data", {}).get("fileUpdate", {}).get("userErrors"):
            errors = result["data"]["fileUpdate"]["userErrors"]
            raise HTTPException(status_code=400, detail=str(errors))
        
        return {
            "success": True,
            "newImage": result.get("data", {}).get("fileUpdate", {}).get("files", [{}])[0]
        }
        
    except Exception as e:
        logger.error(f"Filename update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rename-image")
async def rename_image(data: ImageRenameRequest):
    """Renomear imagem com reupload otimizado"""
    try:
        store_name = data.connection['store_name'].replace('.myshopify.com', '')
        access_token = data.connection['access_token']
        api_version = '2024-04'
        
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
        create_url = f"https://{store_name}.myshopify.com/admin/api/{api_version}/products/{data.productId}/images.json"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Step 1: Criar imagem temporária
            temp_payload = {
                "image": {
                    "filename": f"{data.newFilename}-temp.png",
                    "attachment": data.attachment
                }
            }
            
            temp_response = await client.post(create_url, json=temp_payload, headers=headers)
            
            if temp_response.status_code != 200:
                raise HTTPException(
                    status_code=temp_response.status_code,
                    detail=f"Failed to create temp image: {temp_response.text}"
                )
            
            temp_image = temp_response.json()["image"]
            
            # Aguardar processamento
            await asyncio.sleep(0.5)
            
            # Step 2: Criar imagem final
            final_payload = {
                "image": {
                    "position": data.position,
                    "src": temp_image["src"],
                    "filename": f"{data.newFilename}.png",
                    "variant_ids": data.variantIds or [],
                    "alt": data.altText
                }
            }
            
            final_response = await client.post(create_url, json=final_payload, headers=headers)
            
            if final_response.status_code != 200:
                # Limpar imagem temporária se falhar
                delete_url = f"https://{store_name}.myshopify.com/admin/api/{api_version}/products/{data.productId}/images/{temp_image['id']}.json"
                await client.delete(delete_url, headers=headers)
                
                raise HTTPException(
                    status_code=final_response.status_code,
                    detail=f"Failed to create final image: {final_response.text}"
                )
            
            final_image = final_response.json()["image"]
            
            # Step 3: Deletar imagens antigas em background
            asyncio.create_task(cleanup_old_images(
                client, store_name, api_version, access_token,
                data.productId, [data.imageId, temp_image['id']]
            ))
            
            return {
                "success": True,
                "message": "Image renamed successfully",
                "newImage": final_image
            }
            
    except Exception as e:
        logger.error(f"Image rename error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def cleanup_old_images(client, store_name, api_version, access_token, product_id, image_ids):
    """Limpar imagens antigas em background"""
    headers = {"X-Shopify-Access-Token": access_token}
    for image_id in image_ids:
        try:
            delete_url = f"https://{store_name}.myshopify.com/admin/api/{api_version}/products/{product_id}/images/{image_id}.json"
            await client.delete(delete_url, headers=headers)
        except:
            pass

# ==================== TASK MANAGEMENT ====================

@app.post("/api/tasks/schedule")
async def schedule_task(task: TaskRequest):
    """Criar/agendar nova tarefa"""
    task_id = task.id or f"task_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}"
    
    task_data = {
        "id": task_id,
        "name": task.name,
        "task_type": task.task_type,
        "config": task.config,
        "status": "scheduled",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "progress": {
            "processed": 0,
            "total": len(task.config.get("productIds", [])) if task.config else 0,
            "successful": 0,
            "failed": 0,
            "percentage": 0
        }
    }
    
    tasks_db[task_id] = task_data
    
    await manager.broadcast_task_update(task_id, {
        "type": "task_created",
        "task": task_data
    })
    
    return {"success": True, "taskId": task_id, "task": task_data}

@app.post("/api/tasks/process")
async def process_task(request: Dict[str, Any], background_tasks: BackgroundTasks):
    """Processar tarefa"""
    task_id = request.get("taskId")
    stream_mode = request.get("streamMode", False)
    
    if not task_id:
        raise HTTPException(status_code=400, detail="taskId is required")
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = tasks_db[task_id]
    task["status"] = "running"
    task["started_at"] = datetime.now().isoformat()
    
    # Broadcast início
    await manager.broadcast_task_update(task_id, {
        "type": "task_started",
        "task": task
    })
    
    if stream_mode:
        # Modo stream (SSE)
        return StreamingResponse(
            process_task_stream(task_id, task["config"]),
            media_type="text/event-stream"
        )
    else:
        # Modo background
        background_tasks.add_task(
            process_products_with_websocket,
            task_id,
            task["config"]
        )
        
        return {
            "success": True,
            "message": "Processing started in background",
            "taskId": task_id,
            "mode": "background"
        }

async def process_task_stream(task_id: str, config: Dict[str, Any]):
    """Processar tarefa com Server-Sent Events"""
    product_ids = config.get("productIds", [])
    operations = config.get("operations", [])
    store_name = config.get("storeName", "").replace('.myshopify.com', '')
    access_token = config.get("accessToken", "")
    
    total = len(product_ids)
    processed = 0
    successful = 0
    failed = 0
    
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            # Verificar cancelamento
            if task_id in tasks_db and tasks_db[task_id].get("status") == "cancelled":
                yield f"data: {json.dumps({'type': 'cancelled'})}\n\n"
                return
            
            try:
                yield f"data: {json.dumps({'type': 'processing', 'current': i+1, 'productId': product_id})}\n\n"
                
                # Processar produto
                result = await process_single_product(
                    client, product_id, operations,
                    store_name, access_token
                )
                
                if result["success"]:
                    successful += 1
                    yield f"data: {json.dumps({'type': 'success', 'product': result['product_title'], 'current': i+1})}\n\n"
                else:
                    failed += 1
                    yield f"data: {json.dumps({'type': 'error', 'error': result['error'], 'current': i+1})}\n\n"
                
            except Exception as e:
                failed += 1
                yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
            
            processed = i + 1
            
            # Atualizar progresso
            if task_id in tasks_db:
                tasks_db[task_id]["progress"] = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "percentage": round((processed / total) * 100)
                }
                tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
            
            await asyncio.sleep(0.2)
    
    # Finalizar
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = "completed"
        tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
    
    yield f"data: {json.dumps({'type': 'complete', 'successful': successful, 'failed': failed})}\n\n"

async def process_products_with_websocket(task_id: str, config: Dict[str, Any]):
    """Processar produtos com atualizações via WebSocket"""
    product_ids = config.get("productIds", [])
    operations = config.get("operations", [])
    store_name = config.get("storeName", "").replace('.myshopify.com', '')
    access_token = config.get("accessToken", "")
    
    total = len(product_ids)
    processed = 0
    successful = 0
    failed = 0
    results = []
    
    logger.info(f"🚀 Processando {total} produtos para tarefa {task_id}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, product_id in enumerate(product_ids):
            # Verificar cancelamento
            if task_id in tasks_db and tasks_db[task_id].get("status") == "cancelled":
                logger.info(f"⏹️ Tarefa {task_id} cancelada")
                await manager.broadcast_task_update(task_id, {
                    "type": "task_cancelled",
                    "task": tasks_db[task_id]
                })
                return
            
            try:
                # Broadcast início do processamento
                await manager.broadcast_task_update(task_id, {
                    "type": "product_processing",
                    "productId": product_id,
                    "current": i + 1,
                    "total": total
                })
                
                # Processar produto
                result = await process_single_product(
                    client, product_id, operations,
                    store_name, access_token
                )
                
                if result["success"]:
                    successful += 1
                    results.append({
                        "product_id": product_id,
                        "product_title": result.get("product_title", ""),
                        "status": "updated",
                        "message": "Success"
                    })
                else:
                    failed += 1
                    results.append({
                        "product_id": product_id,
                        "status": "failed",
                        "message": result.get("error", "Unknown error")
                    })
                
                # ATUALIZAR PROGRESSO A CADA PRODUTO
                processed = i + 1
                progress = {
                    "processed": processed,
                    "total": total,
                    "successful": successful,
                    "failed": failed,
                    "percentage": round((processed / total) * 100)
                }
                
                # Atualizar na memória
                if task_id in tasks_db:
                    tasks_db[task_id]["progress"] = progress
                    tasks_db[task_id]["results"] = results[-10:]  # Últimos 10 resultados
                    tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
                
                # BROADCAST VIA WEBSOCKET
                await manager.broadcast_task_update(task_id, {
                    "type": "progress_update",
                    "task": tasks_db[task_id],
                    "lastProduct": {
                        "id": product_id,
                        "title": result.get("product_title", ""),
                        "result": "success" if result["success"] else "failed"
                    }
                })
                
                logger.info(f"📊 Tarefa {task_id}: {processed}/{total} ({progress['percentage']}%)")
                
            except Exception as e:
                failed += 1
                logger.error(f"❌ Erro no produto {product_id}: {e}")
                
                await manager.broadcast_task_update(task_id, {
                    "type": "product_error",
                    "productId": product_id,
                    "error": str(e)
                })
            
            # Rate limiting
            await asyncio.sleep(0.2)
    
    # Finalizar tarefa
    if task_id in tasks_db:
        tasks_db[task_id]["status"] = "completed"
        tasks_db[task_id]["completed_at"] = datetime.now().isoformat()
        
        await manager.broadcast_task_update(task_id, {
            "type": "task_completed",
            "task": tasks_db[task_id]
        })
    
    logger.info(f"🎉 Tarefa {task_id} concluída! ✅ {successful} | ❌ {failed}")

async def process_single_product(client, product_id, operations, store_name, access_token):
    """Processar um único produto"""
    try:
        product_url = f"https://{store_name}.myshopify.com/admin/api/2024-04/products/{product_id}.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
        # GET produto
        response = await client.get(product_url, headers=headers)
        
        if response.status_code != 200:
            return {"success": False, "error": f"Failed to fetch: {response.status_code}"}
        
        product_data = response.json()
        current_product = product_data.get("product", {})
        
        # Preparar update
        update_payload = {"product": {"id": product_id}}
        
        # Aplicar operações
        for op in operations:
            field = op.get("field")
            value = op.get("value")
            
            if field == "title":
                update_payload["product"]["title"] = value
            elif field == "description" or field == "body_html":
                update_payload["product"]["body_html"] = value
            elif field == "vendor":
                update_payload["product"]["vendor"] = value
            elif field == "product_type":
                update_payload["product"]["product_type"] = value
            elif field == "status":
                update_payload["product"]["status"] = value
            elif field == "tags":
                if op.get("meta", {}).get("mode") == "replace":
                    update_payload["product"]["tags"] = value
                else:
                    current_tags = current_product.get("tags", "").split(',')
                    new_tags = value.split(',') if value else []
                    all_tags = list(set([t.strip() for t in current_tags + new_tags if t.strip()]))
                    update_payload["product"]["tags"] = ", ".join(all_tags)
            elif field in ["price", "compare_at_price", "sku"]:
                # Atualizar variantes
                if "variants" not in update_payload["product"] and current_product.get("variants"):
                    update_payload["product"]["variants"] = []
                    for variant in current_product["variants"]:
                        variant_update = {"id": variant["id"]}
                        if field == "price":
                            variant_update["price"] = str(value)
                        elif field == "compare_at_price":
                            variant_update["compare_at_price"] = str(value)
                        elif field == "sku":
                            variant_update["sku"] = value
                        update_payload["product"]["variants"].append(variant_update)
        
        # PUT atualização
        update_response = await client.put(
            product_url,
            headers=headers,
            json=update_payload
        )
        
        if update_response.status_code == 200:
            return {
                "success": True,
                "product_title": current_product.get("title", "")
            }
        else:
            return {
                "success": False,
                "error": f"Update failed: {update_response.status_code}"
            }
            
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== WEBSOCKET ====================

@app.websocket("/ws/task/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    """WebSocket para atualizações em tempo real"""
    await manager.connect(websocket, task_id)
    try:
        # Enviar status inicial
        if task_id in tasks_db:
            await websocket.send_text(json.dumps({
                "type": "initial",
                "task": tasks_db[task_id]
            }))
        
        while True:
            # Manter conexão viva
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"type": "ping"}))
            
    except WebSocketDisconnect:
        manager.disconnect(websocket, task_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket, task_id)

# ==================== STATUS & MANAGEMENT ====================

@app.get("/api/tasks/status")
async def get_all_tasks():
    """Listar todas as tarefas"""
    return list(tasks_db.values())

@app.get("/api/tasks/status/{task_id}")
async def get_task_status(task_id: str, heartbeat: Optional[bool] = False):
    """Status de uma tarefa específica"""
    if task_id not in tasks_db:
        # Retornar tarefa vazia ao invés de 404
        return {
            "id": task_id,
            "status": "not_found",
            "progress": {"processed": 0, "total": 0, "successful": 0, "failed": 0, "percentage": 0}
        }
    
    task = tasks_db[task_id]
    
    # Atualizar heartbeat se solicitado
    if heartbeat:
        task["last_heartbeat"] = datetime.now().isoformat()
    
    return task

@app.post("/api/tasks/cancel")
async def cancel_task(request: Dict[str, str]):
    """Cancelar tarefa"""
    task_id = request.get("taskId")
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    
    tasks_db[task_id]["status"] = "cancelled"
    tasks_db[task_id]["cancelled_at"] = datetime.now().isoformat()
    
    await manager.broadcast_task_update(task_id, {
        "type": "task_cancelled",
        "task": tasks_db[task_id]
    })
    
    return {"success": True, "task": tasks_db[task_id]}

@app.put("/api/tasks/update")
async def update_task(request: Dict[str, Any]):
    """Atualizar tarefa"""
    task_id = request.get("taskId")
    
    if not task_id:
        raise HTTPException(status_code=400, detail="taskId is required")
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Atualizar campos
    for key in ["status", "progress", "error_message"]:
        if key in request:
            tasks_db[task_id][key] = request[key]
    
    tasks_db[task_id]["updated_at"] = datetime.now().isoformat()
    
    await manager.broadcast_task_update(task_id, {
        "type": "task_updated",
        "task": tasks_db[task_id]
    })
    
    return {"success": True, "task": tasks_db[task_id]}

@app.post("/api/tasks/continue")
async def continue_task(request: Dict[str, Any], background_tasks: BackgroundTasks):
    """Continuar tarefa pausada ou morta"""
    task_id = request.get("taskId")
    force = request.get("force", False)
    
    if task_id not in tasks_db:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = tasks_db[task_id]
    
    # Se force=true, forçar continuação
    if force or task["status"] in ["paused", "running"]:
        task["status"] = "running"
        task["updated_at"] = datetime.now().isoformat()
        
        # Reprocessar em background
        background_tasks.add_task(
            process_products_with_websocket,
            task_id,
            task["config"]
        )
        
        return {"success": True, "message": "Task continuation scheduled"}
    
    return {"success": False, "message": f"Task status is {task['status']}"}

@app.delete("/api/tasks/delete")
async def delete_task(task_id: Optional[str] = None):
    """Deletar tarefa"""
    if not task_id:
        # Pegar do body se não vier na query
        return {"error": "taskId is required"}, 400
    
    if task_id in tasks_db:
        del tasks_db[task_id]
        return {"success": True, "message": "Task deleted", "taskId": task_id}
    
    return {"error": "Task not found", "taskId": task_id}, 404

@app.post("/api/tasks/cleanup")
async def cleanup_tasks(request: Dict[str, Any]):
    """Limpar tarefas antigas"""
    delete_completed = request.get("deleteCompleted", True)
    delete_failed = request.get("deleteFailed", True)
    delete_cancelled = request.get("deleteCancelled", True)
    older_than_hours = request.get("olderThanHours", 24)
    dry_run = request.get("dryRun", False)
    
    now = datetime.now()
    deleted = []
    kept = []
    
    for task_id, task in list(tasks_db.items()):
        task_age = (now - datetime.fromisoformat(task["created_at"])).total_seconds() / 3600
        
        should_delete = False
        reason = ""
        
        if task["status"] == "completed" and delete_completed and task_age > older_than_hours:
            should_delete = True
            reason = f"Completed task older than {older_than_hours} hours"
        elif task["status"] == "failed" and delete_failed:
            should_delete = True
            reason = "Failed task"
        elif task["status"] == "cancelled" and delete_cancelled:
            should_delete = True
            reason = "Cancelled task"
        elif task["status"] == "running" and task_age > 2:
            should_delete = True
            reason = "Zombie task"
        
        if should_delete:
            if not dry_run:
                del tasks_db[task_id]
            deleted.append({
                "id": task_id,
                "name": task.get("name", ""),
                "status": task["status"],
                "age": round(task_age, 1),
                "reason": reason
            })
        else:
            kept.append({
                "id": task_id,
                "name": task.get("name", ""),
                "status": task["status"],
                "age": round(task_age, 1)
            })
    
    return {
        "success": True,
        "message": "Dry run completed" if dry_run else "Cleanup completed",
        "stats": {
            "deleted": len(deleted),
            "kept": len(kept),
            "total": len(deleted) + len(kept)
        },
        "deletedTasks": deleted[:10],
        "keptTasks": kept[:10],
        "dryRun": dry_run
    }

@app.get("/api/tasks/manage")
async def manage_tasks(action: Optional[str] = None):
    """Gerenciar tarefas"""
    if action == "clear-all":
        count = len(tasks_db)
        tasks_db.clear()
        return {"success": True, "message": "All tasks cleared", "deleted": count}
    
    # Estatísticas
    stats = {
        "total": len(tasks_db),
        "running": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
        "scheduled": 0,
        "paused": 0
    }
    
    tasks = []
    now = datetime.now()
    
    for task_id, task in tasks_db.items():
        status = task.get("status", "unknown")
        stats[status] = stats.get(status, 0) + 1
        
        age = (now - datetime.fromisoformat(task.get("created_at", now.isoformat()))).total_seconds() / 3600
        
        tasks.append({
            "id": task_id,
            "name": task.get("name", ""),
            "status": status,
            "type": task.get("task_type", ""),
            "created": task.get("created_at", ""),
            "updated": task.get("updated_at", ""),
            "ageHours": round(age, 1),
            "progress": task.get("progress", {}),
            "error": task.get("error_message", "")
        })
    
    # Ordenar por atualização mais recente
    tasks.sort(key=lambda x: x["updated"], reverse=True)
    
    return {
        "success": True,
        "stats": stats,
        "tasks": tasks,
        "timestamp": now.isoformat()
    }

if __name__ == "__main__":
    port = 8000
    logger.info(f"🚀 Iniciando servidor COMPLETO na porta {port}")
    logger.info(f"📦 Recursos disponíveis:")
    logger.info(f"   - Proxy Shopify")
    logger.info(f"   - OAuth")
    logger.info(f"   - Processamento de tarefas")
    logger.info(f"   - WebSockets")
    logger.info(f"   - Operações de imagem")
    uvicorn.run(app, host="0.0.0.0", port=port)