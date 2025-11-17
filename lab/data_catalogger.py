#!/usr/bin/env python3
"""
Catalogador Automático de Dados
Tarefa 3: Integração entre PostgreSQL e Apache Atlas
"""

import logging
from typing import Dict, List, Any, Optional
from atlas_client import AtlasClient, AtlasClientError
from postgres_extractor import PostgreSQLExtractor, PostgreSQLExtractorError

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataCataloggerError(Exception):
    """Exceção customizada para erros do Data Catalogger"""
    pass


class DataCatalogger:
    """
    Catalogador automático de dados que integra PostgreSQL com Apache Atlas.
    
    Cria automaticamente entidades no Atlas representando databases, tabelas,
    colunas e relacionamentos extraídos do PostgreSQL.
    
    Attributes:
        atlas_client (AtlasClient): Cliente para interação com Apache Atlas
        pg_extractor (PostgreSQLExtractor): Extrator de metadados PostgreSQL
        cluster_name (str): Nome do cluster para qualifiedNames
    """
    
    def __init__(self, atlas_client: AtlasClient, 
                 pg_extractor: PostgreSQLExtractor,
                 cluster_name: str = "cluster1"):
        """
        Inicializa o catalogador.
        
        Args:
            atlas_client: Instância do AtlasClient
            pg_extractor: Instância do PostgreSQLExtractor
            cluster_name: Nome do cluster para construir qualified names
        """
        self.atlas = atlas_client
        self.extractor = pg_extractor
        self.cluster_name = cluster_name
        self.created_entities = {}  # Cache de entidades criadas {qualified_name: guid}
        logger.info("DataCatalogger inicializado")
    
    def _build_qualified_name(self, *parts: str) -> str:
        """
        Constrói um qualified name único para entidades Atlas.
        
        Args:
            *parts: Partes do nome a concatenar
            
        Returns:
            String do qualified name no formato: part1.part2.part3@cluster
        """
        name = ".".join(str(p) for p in parts if p)
        return f"{name}@{self.cluster_name}"
    
    def create_database(self, database_name: str, schema: str = "public",
                       description: str = None) -> Dict[str, Any]:
        """
        Cria entidade de database no Atlas.
        
        Args:
            database_name: Nome do banco de dados
            schema: Nome do schema
            description: Descrição opcional
            
        Returns:
            Dict com informações da entidade criada (incluindo GUID)
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            qualified_name = self._build_qualified_name(database_name, schema)
            
            # Verificar se database já existe
            existing = self.atlas.get_entity_by_qualified_name("hive_db", qualified_name)
            if existing:
                guid = existing['entity']['guid']
                logger.info(f"Database '{database_name}' já existe (GUID: {guid})")
                self.created_entities[qualified_name] = guid
                return existing
            
            # Criar nova entidade de database
            db_entity = {
                "typeName": "hive_db",
                "attributes": {
                    "name": f"{database_name}_{schema}",
                    "qualifiedName": qualified_name,
                    "clusterName": self.cluster_name,
                    "description": description or f"Database {database_name}, schema {schema}",
                    "owner": "postgres",
                    "ownerType": "USER",
                    "parameters": {
                        "source": "PostgreSQL",
                        "database": database_name,
                        "schema": schema
                    }
                }
            }
            
            result = self.atlas.create_entity(db_entity)
            
            # Extrair GUID
            guid = None
            if 'guidAssignments' in result:
                guid = result['guidAssignments'].get(qualified_name)
            if not guid and 'mutatedEntities' in result:
                created = result['mutatedEntities'].get('CREATE', [])
                if created:
                    guid = created[0].get('guid')
            
            if guid:
                self.created_entities[qualified_name] = guid
                logger.info(f"✅ Database '{database_name}' criado (GUID: {guid})")
            
            return result
            
        except AtlasClientError as e:
            error_msg = f"Erro ao criar database no Atlas: {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e
    
    def create_table(self, table_metadata: Dict[str, Any], 
                    database_guid: str) -> Dict[str, Any]:
        """
        Cria entidade de tabela no Atlas.
        
        Args:
            table_metadata: Metadados da tabela (do PostgreSQLExtractor)
            database_guid: GUID do database pai
            
        Returns:
            Dict com informações da entidade criada
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            table_name = table_metadata['table_name']
            schema = table_metadata['schema']
            database_name = self.extractor.config['database']
            
            qualified_name = self._build_qualified_name(database_name, schema, table_name)
            
            # Verificar se tabela já existe
            existing = self.atlas.get_entity_by_qualified_name("hive_table", qualified_name)
            if existing:
                guid = existing['entity']['guid']
                logger.info(f"Tabela '{table_name}' já existe (GUID: {guid})")
                self.created_entities[qualified_name] = guid
                return existing
            
            # Construir descrição
            description = (f"Tabela {table_name} com {table_metadata['column_count']} colunas")
            if table_metadata['primary_keys']:
                description += f", PK: {', '.join(table_metadata['primary_keys'])}"
            if table_metadata['foreign_key_count'] > 0:
                description += f", {table_metadata['foreign_key_count']} FK(s)"
            
            # Criar entidade de tabela
            table_entity = {
                "typeName": "hive_table",
                "attributes": {
                    "name": table_name,
                    "qualifiedName": qualified_name,
                    "owner": "postgres",
                    "description": description,
                    "tableType": "MANAGED_TABLE",
                    "db": {"guid": database_guid},
                    "parameters": {
                        "source": "PostgreSQL",
                        "schema": schema,
                        "column_count": str(table_metadata['column_count']),
                        "has_primary_key": str(table_metadata['has_primary_key']),
                        "foreign_key_count": str(table_metadata['foreign_key_count'])
                    }
                }
            }
            
            result = self.atlas.create_entity(table_entity)
            
            # Extrair GUID
            guid = None
            if 'guidAssignments' in result:
                guid = result['guidAssignments'].get(qualified_name)
            if not guid and 'mutatedEntities' in result:
                created = result['mutatedEntities'].get('CREATE', [])
                if created:
                    guid = created[0].get('guid')
            
            if guid:
                self.created_entities[qualified_name] = guid
                logger.info(f"✅ Tabela '{table_name}' criada (GUID: {guid})")
            
            return result
            
        except AtlasClientError as e:
            error_msg = f"Erro ao criar tabela no Atlas: {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e
    
    def create_columns(self, table_metadata: Dict[str, Any],
                      table_guid: str) -> List[Dict[str, Any]]:
        """
        Cria entidades de colunas no Atlas vinculadas a uma tabela.
        
        Args:
            table_metadata: Metadados da tabela
            table_guid: GUID da tabela pai
            
        Returns:
            Lista de resultados da criação das colunas
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            table_name = table_metadata['table_name']
            schema = table_metadata['schema']
            database_name = self.extractor.config['database']
            columns = table_metadata['columns']
            primary_keys = table_metadata['primary_keys']
            
            column_entities = []
            
            for col in columns:
                col_name = col['column_name']
                qualified_name = self._build_qualified_name(
                    database_name, schema, table_name, col_name
                )
                
                # Verificar se coluna já existe
                existing = self.atlas.get_entity_by_qualified_name("hive_column", qualified_name)
                if existing:
                    logger.debug(f"Coluna '{col_name}' já existe")
                    continue
                
                # Determinar se é chave primária
                is_pk = col_name in primary_keys
                
                # Construir descrição
                description = f"Coluna {col_name} ({col['data_type']})"
                if is_pk:
                    description += " - PRIMARY KEY"
                if col['is_nullable'] == 'NO':
                    description += " - NOT NULL"
                
                column_entity = {
                    "typeName": "hive_column",
                    "attributes": {
                        "name": col_name,
                        "qualifiedName": qualified_name,
                        "type": col['data_type'],
                        "description": description,
                        "table": {"guid": table_guid},
                        "position": col['ordinal_position'],
                        "isNullable": col['is_nullable'] == 'YES',
                        "isPrimaryKey": is_pk,
                        "comment": col.get('column_default', '')
                    }
                }
                
                column_entities.append(column_entity)
            
            # Criar colunas em bulk se houver
            if column_entities:
                result = self.atlas.create_entities_bulk(column_entities)
                logger.info(f"✅ {len(column_entities)} colunas criadas para tabela '{table_name}'")
                return [result]
            else:
                logger.info(f"Todas as colunas da tabela '{table_name}' já existem")
                return []
            
        except AtlasClientError as e:
            error_msg = f"Erro ao criar colunas no Atlas: {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e
    
    def create_lineage(self, source_table: str, target_table: str,
                      schema: str = "public") -> Optional[Dict[str, Any]]:
        """
        Cria linhagem de dados entre duas tabelas (relacionamento FK).
        
        Args:
            source_table: Nome da tabela origem (com FK)
            target_table: Nome da tabela destino (referenciada)
            schema: Nome do schema
            
        Returns:
            Dict com resultado da criação da linhagem ou None
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            database_name = self.extractor.config['database']
            
            # Obter GUIDs das tabelas
            source_qn = self._build_qualified_name(database_name, schema, source_table)
            target_qn = self._build_qualified_name(database_name, schema, target_table)
            
            source_guid = self.created_entities.get(source_qn)
            target_guid = self.created_entities.get(target_qn)
            
            if not source_guid or not target_guid:
                logger.warning(f"Não foi possível criar linhagem: GUIDs não encontrados")
                return None
            
            # Criar processo de linhagem
            process_qn = self._build_qualified_name(
                "lineage", database_name, f"{source_table}_to_{target_table}"
            )
            
            process_entity = {
                "typeName": "Process",
                "attributes": {
                    "name": f"{source_table}_to_{target_table}",
                    "qualifiedName": process_qn,
                    "description": f"Relacionamento FK entre {source_table} e {target_table}",
                    "inputs": [{"guid": source_guid}],
                    "outputs": [{"guid": target_guid}]
                }
            }
            
            result = self.atlas.create_entity(process_entity)
            logger.info(f"✅ Linhagem criada: {source_table} → {target_table}")
            return result
            
        except AtlasClientError as e:
            logger.warning(f"Erro ao criar linhagem: {e}")
            return None
    
    def catalog_table(self, table_name: str, database_guid: str,
                     schema: str = "public") -> Dict[str, Any]:
        """
        Cataloga uma tabela completa (tabela + colunas).
        
        Args:
            table_name: Nome da tabela
            database_guid: GUID do database pai
            schema: Nome do schema
            
        Returns:
            Dict com estatísticas da catalogação
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            logger.info(f"📋 Catalogando tabela '{table_name}'...")
            
            # Obter metadados da tabela
            table_metadata = self.extractor.get_table_metadata(table_name, schema)
            
            # Criar tabela
            table_result = self.create_table(table_metadata, database_guid)
            
            # Obter GUID da tabela
            database_name = self.extractor.config['database']
            table_qn = self._build_qualified_name(database_name, schema, table_name)
            table_guid = self.created_entities.get(table_qn)
            
            # Criar colunas
            columns_created = 0
            if table_guid:
                column_results = self.create_columns(table_metadata, table_guid)
                columns_created = len(column_results)
            
            return {
                "table_name": table_name,
                "table_created": True,
                "columns_created": columns_created,
                "metadata": table_metadata
            }
            
        except Exception as e:
            error_msg = f"Erro ao catalogar tabela '{table_name}': {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e
    
    def catalog_all_tables(self, schema: str = "public") -> Dict[str, Any]:
        """
        Cataloga todas as tabelas do schema no Atlas.
        
        Este é o método principal que:
        1. Cria o database no Atlas
        2. Extrai metadados de todas as tabelas
        3. Cria entidades para cada tabela e suas colunas
        4. Cria linhagem de dados baseada em FKs
        
        Args:
            schema: Nome do schema a catalogar (padrão: 'public')
            
        Returns:
            Dict com estatísticas completas da catalogação
            
        Raises:
            DataCataloggerError: Em caso de erro
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 Iniciando catalogação completa do banco de dados")
            logger.info("=" * 60)
            
            database_name = self.extractor.config['database']
            
            # 1. Criar database
            logger.info(f"\n📂 Criando database '{database_name}'...")
            db_result = self.create_database(database_name, schema)
            db_qn = self._build_qualified_name(database_name, schema)
            database_guid = self.created_entities.get(db_qn)
            
            if not database_guid:
                raise DataCataloggerError("Falha ao obter GUID do database")
            
            # 2. Obter todas as tabelas
            logger.info(f"\n🔍 Extraindo metadados de todas as tabelas...")
            all_metadata = self.extractor.get_all_tables_metadata(schema)
            total_tables = len(all_metadata)
            logger.info(f"Encontradas {total_tables} tabelas para catalogar")
            
            # 3. Catalogar cada tabela
            logger.info(f"\n📋 Catalogando tabelas e colunas...")
            tables_created = 0
            columns_created = 0
            errors = []
            
            for table_name, metadata in all_metadata.items():
                try:
                    result = self.catalog_table(table_name, database_guid, schema)
                    if result['table_created']:
                        tables_created += 1
                    columns_created += result['columns_created']
                except Exception as e:
                    logger.error(f"Erro ao catalogar '{table_name}': {e}")
                    errors.append({"table": table_name, "error": str(e)})
            
            # 4. Criar linhagem baseada em FKs
            logger.info(f"\n🔗 Criando linhagem de dados...")
            relationships_df = self.extractor.get_table_relationships(schema)
            lineages_created = 0
            
            for _, rel in relationships_df.iterrows():
                try:
                    result = self.create_lineage(
                        rel['source_table'],
                        rel['target_table'],
                        schema
                    )
                    if result:
                        lineages_created += 1
                except Exception as e:
                    logger.warning(f"Erro ao criar linhagem: {e}")
            
            # 5. Compilar resultados
            results = {
                "database_name": database_name,
                "schema": schema,
                "database_guid": database_guid,
                "total_tables_found": total_tables,
                "tables_created": tables_created,
                "columns_created": columns_created,
                "lineages_created": lineages_created,
                "errors": errors,
                "success_rate": f"{(tables_created/total_tables*100):.1f}%" if total_tables > 0 else "0%"
            }
            
            # Log do resumo
            logger.info("\n" + "=" * 60)
            logger.info("✅ CATALOGAÇÃO CONCLUÍDA")
            logger.info("=" * 60)
            logger.info(f"Database: {database_name}")
            logger.info(f"Tabelas catalogadas: {tables_created}/{total_tables}")
            logger.info(f"Colunas criadas: {columns_created}")
            logger.info(f"Linhagens criadas: {lineages_created}")
            logger.info(f"Taxa de sucesso: {results['success_rate']}")
            if errors:
                logger.warning(f"Erros encontrados: {len(errors)}")
            logger.info("=" * 60)
            
            return results
            
        except PostgreSQLExtractorError as e:
            error_msg = f"Erro ao extrair metadados do PostgreSQL: {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e
        except Exception as e:
            error_msg = f"Erro durante catalogação: {e}"
            logger.error(error_msg)
            raise DataCataloggerError(error_msg) from e


def main():
    """Exemplo de uso do catalogador"""
    from config import ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD, POSTGRES_CONFIG
    
    print("🎯 Data Catalogger - PostgreSQL → Apache Atlas")
    print("=" * 60)
    
    try:
        # Inicializar componentes
        print("\n🔧 Inicializando componentes...")
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        extractor = PostgreSQLExtractor(**POSTGRES_CONFIG)
        catalogger = DataCatalogger(atlas, extractor)
        
        # Testar conexão
        print("✅ AtlasClient inicializado")
        version = atlas.get_version()
        print(f"   Atlas versão: {version.get('Version', 'N/A')}")
        
        print("✅ PostgreSQLExtractor inicializado")
        
        # Catalogar todas as tabelas
        print("\n🚀 Iniciando catalogação automática...")
        results = catalogger.catalog_all_tables()
        
        # Mostrar resultados
        print(f"\n📊 RESULTADOS:")
        print(f"  Database: {results['database_name']}")
        print(f"  Tabelas: {results['tables_created']}/{results['total_tables_found']}")
        print(f"  Colunas: {results['columns_created']}")
        print(f"  Linhagens: {results['lineages_created']}")
        print(f"  Sucesso: {results['success_rate']}")
        
        if results['errors']:
            print(f"\n⚠️  Erros ({len(results['errors'])}):")
            for err in results['errors'][:3]:  # Mostrar apenas primeiros 3
                print(f"  - {err['table']}: {err['error']}")
        
        print("\n✅ Catalogação concluída!")
        
    except DataCataloggerError as e:
        print(f"\n❌ Erro no catalogador: {e}")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
    finally:
        # Fechar conexão
        if 'extractor' in locals():
            extractor.disconnect()


if __name__ == "__main__":
    main()
