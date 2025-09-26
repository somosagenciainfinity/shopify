const express = require('express');
const cors = require('cors');
const app = express();

// Configurar CORS para permitir requisições do seu app React
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: '*',
  credentials: true
}));

// Middleware para parsing
app.use(express.json({ limit: '50mb' }));
app.use(express.raw({ limit: '50mb' }));
app.use(express.text({ limit: '50mb' }));

// Health check
app.get('/', (req, res) => {
  res.json({ 
    status: 'healthy',
    service: 'shopify-proxy-railway',
    timestamp: new Date().toISOString()
  });
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    service: 'shopify-proxy-railway',
    timestamp: new Date().toISOString()
  });
});

// ROTA PRINCIPAL DE PROXY
app.all('/proxy', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ 
      error: 'Missing URL parameter' 
    });
  }

  console.log(`[PROXY] ${req.method} ${targetUrl}`);
  
  try {
    const headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    
    // Copiar o token do Shopify se existir
    const token = req.headers['x-shopify-access-token'];
    if (token) {
      headers['X-Shopify-Access-Token'] = token;
    }

    // Preparar body
    let body = undefined;
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      if (typeof req.body === 'object') {
        body = JSON.stringify(req.body);
      } else {
        body = req.body;
      }
    }

    // Fazer requisição para o Shopify
    const response = await fetch(targetUrl, {
      method: req.method,
      headers: headers,
      body: body
    });

    const responseText = await response.text();
    
    // Tentar parsear como JSON
    try {
      const responseJson = JSON.parse(responseText);
      res.status(response.status).json(responseJson);
    } catch {
      res.status(response.status).send(responseText);
    }

  } catch (error) {
    console.error('[PROXY ERROR]', error);
    res.status(500).json({ 
      error: 'Proxy failed', 
      message: error.message 
    });
  }
});

// Porta
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Proxy server running on port ${PORT}`);
  console.log(`📡 Ready at https://shopify-production-8bcd.up.railway.app`);
});