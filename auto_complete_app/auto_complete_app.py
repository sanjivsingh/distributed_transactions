import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import threading
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn
from commons import logger
from shard_manager import ShardManager, ShardConfig
from database_client import MongoShardClient
from contextlib import asynccontextmanager

log = logger.setup_logger(__name__)


class AutoCompleteRequest(BaseModel):
    query: str

class AutoCompleteResponse(BaseModel):
    query: str
    suggestions: List[str]
    shard_used: str
    response_time_ms: float

class UserSearchService:
    """Stateless service for handling user search requests"""
    
    def __init__(self):
        self.shard_manager = ShardManager()
        self.current_config: Optional[ShardConfig] = None
        self.shard_clients: Dict[str, MongoShardClient] = {}
        self.config_lock = threading.RLock()
        
        # Load initial configuration
        self.__refresh_config()
        
        # Watch for configuration changes
        self.shard_manager.watch_shard_config(self.on_config_change)
    
    def __refresh_config(self):
        """Refresh shard configuration from Zookeeper"""
        try:
            with self.config_lock:
                config_data = self.shard_manager.get_shard_config()
                if config_data:
                    self.current_config = config_data
                    self.__update_shard_clients()
                    log.info("Shard configuration refreshed")
                else:
                    log.warning("No shard configuration available")
                    
        except Exception as e:
            log.error(f"Error refreshing config: {e}")
    
    def on_config_change(self, new_config: ShardConfig):
        """Handle shard configuration changes"""
        try:
            with self.config_lock:
                self.current_config = new_config
                self.__update_shard_clients()
                log.info(f"Configuration updated to version: {new_config.shard_version}")
                
        except Exception as e:
            log.error(f"Error handling config change: {e}")
    
    def __update_shard_clients(self):
        """Update MongoDB shard clients based on current configuration"""
        try:
            if not self.current_config:
                log.error("No current configuration to update shard clients")
                return
            
            # Close existing clients
            for client in self.shard_clients.values():
                if hasattr(client, 'client') and client.client:
                    client.client.close()
            
            # Create new clients
            new_clients = {}
            for mapping in self.current_config.mappings:
                try:
                    client = MongoShardClient(mapping.node, mapping.data_base, mapping.collection)
                    new_clients[mapping.prefix] = client
                    log.info(f"Created shard client for prefix '{mapping.prefix}' -> {mapping.node}:{mapping.data_base}/{mapping.collection}")
                except Exception as e:
                    log.error(f"Failed to create client for {mapping.node}: {e}")
            
            self.shard_clients = new_clients
            
        except Exception as e:
            log.error(f"Error updating shard clients: {e}")
    
    def __find_shard_for_prefix(self, query: str) -> Tuple[str, MongoShardClient]:
        """Find the appropriate shard for a given query prefix"""
        try:
            if not self.current_config or not query:
                raise ValueError("Invalid configuration or query")
            shard_info = self.shard_manager.find_shard_for_prefix(query)
            if shard_info:
                prefix = shard_info["prefix"]
                if prefix not in self.shard_clients:
                    node = shard_info["node"]
                    data_base = shard_info["data_base"]
                    collection = shard_info["collection"]
                    self.shard_clients[prefix] = MongoShardClient(node, data_base, collection)

                return prefix, self.shard_clients[prefix]
            raise ValueError("No matching shard found for the given prefix")
            
        except Exception as e:
            raise RuntimeError(f"Error finding shard for prefix '{query}': {e}")
    
    def get_suggestions(self, query: str) -> AutoCompleteResponse:
        """Get auto-complete suggestions for a query"""
        start_time = time.time()
        
        try:
            with self.config_lock:
                shard_node, shard_client = self.__find_shard_for_prefix(query)
                shard_used = shard_node if shard_node else "no_shard_found"
                suggestions = shard_client.get_suggestions(query) if shard_client else []
                response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                return AutoCompleteResponse(
                    query=query,
                    suggestions=suggestions,
                    shard_used=shard_used,
                    response_time_ms=round(response_time, 2)
                )
                
        except Exception as e:
            log.error(f"Error getting suggestions for '{query}': {e}")
            response_time = (time.time() - start_time) * 1000
            
            return AutoCompleteResponse(
                query=query,
                suggestions=[],
                shard_used="error",
                response_time_ms=round(response_time, 2)
            )

# Use lifespan instead of on_event for FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global search_service
    try:
        search_service = UserSearchService()
        log.info("Auto-complete service initialized successfully")
    except Exception as e:
        log.error(f"Failed to initialize service: {e}")
        raise
    
    yield
    
    # Shutdown
    if search_service:
        # Close any resources if needed
        try:
            if hasattr(search_service, 'shard_manager'):
                search_service.shard_manager.close()
            log.info("Auto-complete service shut down successfully")
        except Exception as e:
            log.error(f"Error during shutdown: {e}")

# Global service instance
search_service: Optional[UserSearchService] = None


app = FastAPI(lifespan=lifespan)


# FastAPI Routes

@app.get("/", response_class=HTMLResponse)
async def root():
    try:
        if not search_service:
            raise HTTPException(status_code=503, detail="Service not initialized")
    except Exception as e:
        log.error(f"Error in root endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    """Serve the main UI"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Auto Complete Service</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                background-color: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
                text-align: center;
            }
            .search-box {
                width: 100%;
                padding: 15px;
                font-size: 18px;
                border: 2px solid #ddd;
                border-radius: 5px;
                box-sizing: border-box;
            }
            .suggestions {
                margin-top: 10px;
                border: 1px solid #ddd;
                border-radius: 5px;
                max-height: 300px;
                overflow-y: auto;
                display: none;
            }
            .suggestion-item {
                padding: 10px;
                cursor: pointer;
                border-bottom: 1px solid #eee;
            }
            .suggestion-item:hover {
                background-color: #f0f0f0;
            }
            .suggestion-item:last-child {
                border-bottom: none;
            }
            .info {
                margin-top: 15px;
                padding: 10px;
                background-color: #e8f4fd;
                border-radius: 5px;
                font-size: 14px;
                display: none;
            }
            .shard-info {
                color: #666;
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Auto Complete Service</h1>            
            <input type="text" id="searchBox" class="search-box" 
                placeholder="Start typing... (try: apple, facebook, javascript, search, technology)" 
                autocomplete="off">
            
            <div id="suggestions" class="suggestions"></div>
            
            <div id="info" class="info">
                <div><strong>Query:</strong> <span id="query"></span></div>
                <div><strong>Response Time:</strong> <span id="responseTime"></span> ms</div>
                <div><strong>Shard Used:</strong> <span id="shardUsed" class="shard-info"></span></div>
                <div><strong>Suggestions Found:</strong> <span id="suggestionsCount"></span></div>
            </div>
        </div>

        <script>
            const searchBox = document.getElementById('searchBox');
            const suggestionsDiv = document.getElementById('suggestions');
            const infoDiv = document.getElementById('info');
            
            let debounceTimer;
            
            searchBox.addEventListener('input', function() {
                clearTimeout(debounceTimer);
                const query = this.value.trim();
                
                if (query.length === 0) {
                    suggestionsDiv.style.display = 'none';
                    infoDiv.style.display = 'none';
                    return;
                }
                
                debounceTimer = setTimeout(() => {
                    fetchSuggestions(query);
                }, 200); // 200ms debounce
            });
            
            async function fetchSuggestions(query) {
                try {
                    const response = await fetch(`/autocomplete?query=${encodeURIComponent(query)}`);
                    const data = await response.json();
                    
                    displaySuggestions(data);
                    displayInfo(data);
                    
                } catch (error) {
                    console.error('Error fetching suggestions:', error);
                    suggestionsDiv.innerHTML = '<div class="suggestion-item">Error fetching suggestions</div>';
                    suggestionsDiv.style.display = 'block';
                }
            }
            
            function displaySuggestions(data) {
                if (data.suggestions.length === 0) {
                    suggestionsDiv.innerHTML = '<div class="suggestion-item">No suggestions found</div>';
                } else {
                    suggestionsDiv.innerHTML = data.suggestions
                        .map(suggestion => `<div class="suggestion-item" onclick="selectSuggestion('${suggestion}')">${suggestion}</div>`)
                        .join('');
                }
                suggestionsDiv.style.display = 'block';
            }
            
            function displayInfo(data) {
                document.getElementById('query').textContent = data.query;
                document.getElementById('responseTime').textContent = data.response_time_ms;
                document.getElementById('shardUsed').textContent = data.shard_used;
                document.getElementById('suggestionsCount').textContent = data.suggestions.length;
                infoDiv.style.display = 'block';
            }
            
            function selectSuggestion(suggestion) {
                searchBox.value = suggestion;
                suggestionsDiv.style.display = 'none';
                fetchSuggestions(suggestion); // Update info for selected suggestion
            }
            
            // Hide suggestions when clicking outside
            document.addEventListener('click', function(event) {
                if (!searchBox.contains(event.target) && !suggestionsDiv.contains(event.target)) {
                    suggestionsDiv.style.display = 'none';
                }
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/autocomplete", response_model=AutoCompleteResponse)
async def autocomplete(query: str):
    """Get auto-complete suggestions"""
    try:
        if not search_service:
            raise HTTPException(status_code=503, detail="Service not initialized")
        
        if not query or len(query.strip()) == 0:
            raise HTTPException(status_code=400, detail="Query cannot be empty")

        result = search_service.get_suggestions(query.strip())
        return JSONResponse(content=result.dict(), headers={"Cache-Control": "public, max-age=300"})
        
    except HTTPException as e:
        log.error(f"Error in autocomplete endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error : {e}")
    except Exception as e:
        log.error(f"Error in autocomplete endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

if __name__ == "__main__":
    try:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="trace")
    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        if search_service:
            try:
                if hasattr(search_service, 'shard_manager'):
                    search_service.shard_manager.close()
                log.info("Auto-complete service shut down successfully")
            except Exception as e:
                log.error(f"Error during shutdown: {e}")

