#!/usr/bin/env python3
"""
Cliente Python para Apache Atlas
Demonstra conexão e operações básicas com a API REST
Tarefa 1: Cliente Atlas completo com tratamento de erros
"""

import requests
import json
import logging
from requests.auth import HTTPBasicAuth
from typing import Dict, List, Optional, Any

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AtlasClientError(Exception):
    """Exceção customizada para erros do Atlas Client"""
    pass


class AtlasClient:
    """
    Cliente para interação com Apache Atlas via API REST.
    
    Implementa métodos para buscar, criar e gerenciar entidades,
    além de obter linhagem de dados.
    
    Attributes:
        url (str): URL base do Apache Atlas
        auth (HTTPBasicAuth): Credenciais de autenticação
        session (requests.Session): Sessão HTTP persistente
    """
    
    def __init__(self, url: str = "http://localhost:21000", 
                 username: str = "admin", 
                 password: str = "admin"):
        """
        Inicializa o cliente Atlas.
        
        Args:
            url: URL do servidor Apache Atlas
            username: Nome de usuário para autenticação
            password: Senha para autenticação
        """
        self.url = url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({'Content-Type': 'application/json'})
        logger.info(f"AtlasClient inicializado para {self.url}")
    
    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Trata resposta HTTP e lança exceções apropriadas.
        
        Args:
            response: Objeto de resposta do requests
            
        Returns:
            Dict contendo o JSON da resposta
            
        Raises:
            AtlasClientError: Em caso de erro HTTP
        """
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = f"Erro HTTP {response.status_code}: {response.text}"
            logger.error(error_msg)
            raise AtlasClientError(error_msg) from e
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Erro de conexão com Atlas em {self.url}"
            logger.error(error_msg)
            raise AtlasClientError(error_msg) from e
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout ao conectar com Atlas"
            logger.error(error_msg)
            raise AtlasClientError(error_msg) from e
        except json.JSONDecodeError as e:
            error_msg = f"Erro ao decodificar resposta JSON: {response.text}"
            logger.error(error_msg)
            raise AtlasClientError(error_msg) from e
    
    def get_version(self) -> Dict[str, Any]:
        """
        Obtém versão do Apache Atlas.
        
        Returns:
            Dict com informações de versão
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.get(f"{self.url}/api/atlas/admin/version")
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Erro ao obter versão: {e}")
            raise
    
    def get_types(self) -> Dict[str, Any]:
        """
        Lista todos os tipos de entidades disponíveis.
        
        Returns:
            Dict contendo definições de tipos
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.get(f"{self.url}/api/atlas/v2/types/typedefs")
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Erro ao obter tipos: {e}")
            raise
    
    def search_entities(self, query: str = "*", limit: int = 10, 
                       type_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Busca entidades por termo de busca.
        
        Args:
            query: Termo de busca (usa * para buscar todas)
            limit: Número máximo de resultados
            type_name: Filtrar por tipo de entidade (opcional)
            
        Returns:
            Dict contendo resultados da busca
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            params = {"query": query, "limit": limit}
            if type_name:
                params["typeName"] = type_name
            
            response = self.session.get(
                f"{self.url}/api/atlas/v2/search/basic", 
                params=params
            )
            result = self._handle_response(response)
            logger.info(f"Busca por '{query}': {len(result.get('entities', []))} entidades encontradas")
            return result
        except Exception as e:
            logger.error(f"Erro ao buscar entidades: {e}")
            raise
    
    def create_entity(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cria uma nova entidade no Atlas.
        
        Args:
            entity_data: Dicionário com dados da entidade
            
        Returns:
            Dict com informações da entidade criada (incluindo GUID)
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.post(
                f"{self.url}/api/atlas/v2/entity",
                json={"entity": entity_data}
            )
            result = self._handle_response(response)
            
            # Extrair GUID da entidade criada
            guid = result.get('guidAssignments', {}).get(entity_data.get('attributes', {}).get('qualifiedName'))
            if not guid and 'mutatedEntities' in result:
                created = result['mutatedEntities'].get('CREATE', [])
                if created:
                    guid = created[0].get('guid')
            
            logger.info(f"Entidade criada: {entity_data.get('attributes', {}).get('name')} (GUID: {guid})")
            return result
        except Exception as e:
            logger.error(f"Erro ao criar entidade: {e}")
            raise
    
    def create_entities_bulk(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Cria múltiplas entidades em uma única requisição.
        
        Args:
            entities: Lista de dicionários com dados das entidades
            
        Returns:
            Dict com informações das entidades criadas
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.post(
                f"{self.url}/api/atlas/v2/entity/bulk",
                json={"entities": entities}
            )
            result = self._handle_response(response)
            logger.info(f"{len(entities)} entidades criadas em bulk")
            return result
        except Exception as e:
            logger.error(f"Erro ao criar entidades em bulk: {e}")
            raise
    
    def get_entity(self, guid: str) -> Dict[str, Any]:
        """
        Obtém entidade pelo GUID.
        
        Args:
            guid: GUID único da entidade
            
        Returns:
            Dict com dados completos da entidade
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.get(f"{self.url}/api/atlas/v2/entity/guid/{guid}")
            result = self._handle_response(response)
            logger.info(f"Entidade obtida: GUID {guid}")
            return result
        except Exception as e:
            logger.error(f"Erro ao obter entidade {guid}: {e}")
            raise
    
    def get_entity_by_qualified_name(self, type_name: str, 
                                    qualified_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtém entidade pelo nome qualificado.
        
        Args:
            type_name: Tipo da entidade (ex: 'hive_table')
            qualified_name: Nome qualificado único
            
        Returns:
            Dict com dados da entidade ou None se não encontrada
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            params = {
                "typeName": type_name,
                "attr:qualifiedName": qualified_name
            }
            response = self.session.get(
                f"{self.url}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}",
                params=params
            )
            if response.status_code == 404:
                return None
            result = self._handle_response(response)
            logger.info(f"Entidade encontrada: {qualified_name}")
            return result
        except AtlasClientError as e:
            if "404" in str(e):
                return None
            raise
        except Exception as e:
            logger.error(f"Erro ao obter entidade por qualified name: {e}")
            raise
    
    def get_lineage(self, guid: str, depth: int = 3, 
                   direction: str = "BOTH") -> Dict[str, Any]:
        """
        Obtém linhagem de dados de uma entidade.
        
        Args:
            guid: GUID da entidade
            depth: Profundidade da linhagem a retornar
            direction: Direção da linhagem ('INPUT', 'OUTPUT', 'BOTH')
            
        Returns:
            Dict contendo informações de linhagem (relações upstream/downstream)
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            params = {"depth": depth, "direction": direction}
            response = self.session.get(
                f"{self.url}/api/atlas/v2/lineage/{guid}",
                params=params
            )
            result = self._handle_response(response)
            logger.info(f"Linhagem obtida para entidade {guid}")
            return result
        except Exception as e:
            logger.error(f"Erro ao obter linhagem: {e}")
            raise
    
    def update_entity(self, guid: str, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Atualiza uma entidade existente.
        
        Args:
            guid: GUID da entidade a atualizar
            entity_data: Dados atualizados da entidade
            
        Returns:
            Dict com resultado da atualização
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            entity_data['guid'] = guid
            response = self.session.put(
                f"{self.url}/api/atlas/v2/entity",
                json={"entity": entity_data}
            )
            result = self._handle_response(response)
            logger.info(f"Entidade atualizada: GUID {guid}")
            return result
        except Exception as e:
            logger.error(f"Erro ao atualizar entidade: {e}")
            raise
    
    def delete_entity(self, guid: str) -> Dict[str, Any]:
        """
        Deleta uma entidade pelo GUID.
        
        Args:
            guid: GUID da entidade a deletar
            
        Returns:
            Dict com resultado da deleção
            
        Raises:
            AtlasClientError: Em caso de erro na requisição
        """
        try:
            response = self.session.delete(f"{self.url}/api/atlas/v2/entity/guid/{guid}")
            result = self._handle_response(response)
            logger.info(f"Entidade deletada: GUID {guid}")
            return result
        except Exception as e:
            logger.error(f"Erro ao deletar entidade: {e}")
            raise


def main():
    """Exemplo de uso do cliente Atlas"""
    print("🚀 Conectando ao Apache Atlas...")
    
    client = AtlasClient()
    
    try:
        # Testar conexão
        version = client.get_version()
        print(f"✅ Atlas versão: {version}")
        
        # Listar tipos
        types = client.get_types()
        entity_types = types.get('entityDefs', [])
        print(f"📋 Tipos disponíveis: {len(entity_types)}")
        
        # Buscar entidades
        results = client.search_entities("*")
        entities = results.get('entities', [])
        print(f"🔍 Entidades encontradas: {len(entities)}")
        
        # Mostrar primeiras entidades
        for i, entity in enumerate(entities[:3]):
            print(f"  {i+1}. {entity.get('displayText', 'N/A')} ({entity.get('typeName', 'N/A')})")
        
        # Testar linhagem (se houver entidades)
        if entities:
            first_guid = entities[0].get('guid')
            if first_guid:
                print(f"\n🔗 Obtendo linhagem para primeira entidade...")
                try:
                    lineage = client.get_lineage(first_guid)
                    print(f"  Relações encontradas: {len(lineage.get('relations', []))}")
                except Exception as e:
                    print(f"  Sem linhagem disponível para esta entidade")
            
    except AtlasClientError as e:
        print(f"❌ Erro do Atlas: {e}")
    except Exception as e:
        print(f"❌ Erro: {e}")


if __name__ == "__main__":
    main()