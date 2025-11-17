#!/usr/bin/env python3
"""
Extrator de Metadados PostgreSQL
Tarefa 2: Extração completa de metadados do banco de dados
"""

import psycopg2
import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from psycopg2.extras import RealDictCursor

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgreSQLExtractorError(Exception):
    """Exceção customizada para erros do PostgreSQL Extractor"""
    pass


class PostgreSQLExtractor:
    """
    Extrator de metadados de banco de dados PostgreSQL.
    
    Extrai informações detalhadas sobre tabelas, colunas, chaves primárias,
    chaves estrangeiras e relacionamentos.
    
    Attributes:
        config (dict): Configuração de conexão com PostgreSQL
        connection: Conexão ativa com o banco (quando conectado)
    """
    
    def __init__(self, host: str = "localhost", port: int = 2001,
                 database: str = "northwind", user: str = "postgres",
                 password: str = "postgres"):
        """
        Inicializa o extrator PostgreSQL.
        
        Args:
            host: Hostname do servidor PostgreSQL
            port: Porta do servidor PostgreSQL
            database: Nome do banco de dados
            user: Usuário para autenticação
            password: Senha para autenticação
        """
        self.config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.connection = None
        logger.info(f"PostgreSQLExtractor inicializado para {database}@{host}:{port}")
    
    def connect(self) -> None:
        """
        Estabelece conexão com o banco de dados.
        
        Raises:
            PostgreSQLExtractorError: Em caso de erro na conexão
        """
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(**self.config)
                logger.info("Conexão estabelecida com PostgreSQL")
        except psycopg2.Error as e:
            error_msg = f"Erro ao conectar ao PostgreSQL: {e}"
            logger.error(error_msg)
            raise PostgreSQLExtractorError(error_msg) from e
    
    def disconnect(self) -> None:
        """Fecha conexão com o banco de dados."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Conexão fechada com PostgreSQL")
    
    def _execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Executa query e retorna resultado como DataFrame.
        
        Args:
            query: SQL query a executar
            params: Parâmetros para query parametrizada
            
        Returns:
            DataFrame com resultados
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro na execução
        """
        try:
            self.connect()
            df = pd.read_sql(query, self.connection, params=params)
            return df
        except Exception as e:
            error_msg = f"Erro ao executar query: {e}"
            logger.error(error_msg)
            raise PostgreSQLExtractorError(error_msg) from e
    
    def get_tables(self, schema: str = "public") -> pd.DataFrame:
        """
        Lista todas as tabelas do schema especificado.
        
        Args:
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            DataFrame com informações das tabelas (table_name, table_schema, table_type)
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        query = """
        SELECT 
            table_name,
            table_schema,
            table_type
        FROM information_schema.tables 
        WHERE table_schema = %s
        ORDER BY table_name
        """
        try:
            df = self._execute_query(query, (schema,))
            logger.info(f"{len(df)} tabelas encontradas no schema '{schema}'")
            return df
        except Exception as e:
            logger.error(f"Erro ao obter tabelas: {e}")
            raise
    
    def get_table_columns(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        """
        Obtém informações detalhadas das colunas de uma tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            DataFrame com informações das colunas (column_name, data_type, 
            is_nullable, column_default, ordinal_position, character_maximum_length)
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns 
        WHERE table_name = %s AND table_schema = %s
        ORDER BY ordinal_position
        """
        try:
            df = self._execute_query(query, (table_name, schema))
            logger.info(f"{len(df)} colunas encontradas na tabela '{table_name}'")
            return df
        except Exception as e:
            logger.error(f"Erro ao obter colunas da tabela {table_name}: {e}")
            raise
    
    def get_primary_keys(self, table_name: str, schema: str = "public") -> List[str]:
        """
        Obtém as colunas que compõem a chave primária de uma tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            Lista com nomes das colunas da chave primária
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        query = """
        SELECT a.attname AS column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
        AND i.indisprimary
        ORDER BY a.attnum
        """
        try:
            self.connect()
            qualified_name = f"{schema}.{table_name}"
            df = self._execute_query(query, (qualified_name,))
            pk_columns = df['column_name'].tolist() if not df.empty else []
            logger.info(f"Chave primária da tabela '{table_name}': {pk_columns}")
            return pk_columns
        except Exception as e:
            logger.error(f"Erro ao obter chave primária de {table_name}: {e}")
            raise
    
    def get_foreign_keys(self, table_name: str, schema: str = "public") -> pd.DataFrame:
        """
        Obtém informações sobre chaves estrangeiras de uma tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            DataFrame com informações das FKs (constraint_name, column_name,
            foreign_table_name, foreign_column_name)
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        query = """
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s
            AND tc.table_schema = %s
        ORDER BY tc.constraint_name
        """
        try:
            df = self._execute_query(query, (table_name, schema))
            logger.info(f"{len(df)} chaves estrangeiras encontradas na tabela '{table_name}'")
            return df
        except Exception as e:
            logger.error(f"Erro ao obter chaves estrangeiras de {table_name}: {e}")
            raise
    
    def get_table_metadata(self, table_name: str, schema: str = "public") -> Dict[str, Any]:
        """
        Obtém todos os metadados de uma tabela (colunas, PKs, FKs).
        
        Args:
            table_name: Nome da tabela
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            Dict estruturado com todos os metadados da tabela
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        try:
            logger.info(f"Extraindo metadados completos da tabela '{table_name}'")
            
            # Obter colunas
            columns_df = self.get_table_columns(table_name, schema)
            
            # Obter chave primária
            primary_keys = self.get_primary_keys(table_name, schema)
            
            # Obter chaves estrangeiras
            foreign_keys_df = self.get_foreign_keys(table_name, schema)
            
            # Estruturar metadados
            metadata = {
                "table_name": table_name,
                "schema": schema,
                "columns": columns_df.to_dict('records'),
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys_df.to_dict('records'),
                "column_count": len(columns_df),
                "has_primary_key": len(primary_keys) > 0,
                "foreign_key_count": len(foreign_keys_df)
            }
            
            logger.info(f"Metadados extraídos: {len(columns_df)} colunas, "
                       f"{len(primary_keys)} PKs, {len(foreign_keys_df)} FKs")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Erro ao obter metadados da tabela {table_name}: {e}")
            raise
    
    def get_all_tables_metadata(self, schema: str = "public") -> Dict[str, Dict[str, Any]]:
        """
        Obtém metadados de todas as tabelas do schema.
        
        Args:
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            Dict com metadados de todas as tabelas {table_name: metadata}
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        try:
            logger.info(f"Extraindo metadados de todas as tabelas do schema '{schema}'")
            
            tables_df = self.get_tables(schema)
            all_metadata = {}
            
            for _, table_row in tables_df.iterrows():
                table_name = table_row['table_name']
                try:
                    metadata = self.get_table_metadata(table_name, schema)
                    all_metadata[table_name] = metadata
                except Exception as e:
                    logger.warning(f"Erro ao processar tabela {table_name}: {e}")
                    continue
            
            logger.info(f"Metadados de {len(all_metadata)} tabelas extraídos com sucesso")
            return all_metadata
            
        except Exception as e:
            logger.error(f"Erro ao obter metadados de todas as tabelas: {e}")
            raise
    
    def get_table_relationships(self, schema: str = "public") -> pd.DataFrame:
        """
        Obtém todos os relacionamentos (FKs) entre tabelas do schema.
        
        Args:
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            DataFrame com relacionamentos (source_table, source_column,
            target_table, target_column, constraint_name)
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        query = """
        SELECT
            tc.table_name AS source_table,
            kcu.column_name AS source_column,
            ccu.table_name AS target_table,
            ccu.column_name AS target_column,
            tc.constraint_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = %s
        ORDER BY tc.table_name, tc.constraint_name
        """
        try:
            df = self._execute_query(query, (schema,))
            logger.info(f"{len(df)} relacionamentos encontrados no schema '{schema}'")
            return df
        except Exception as e:
            logger.error(f"Erro ao obter relacionamentos: {e}")
            raise
    
    def get_database_summary(self, schema: str = "public") -> Dict[str, Any]:
        """
        Gera um resumo estatístico do banco de dados.
        
        Args:
            schema: Nome do schema (padrão: 'public')
            
        Returns:
            Dict com estatísticas do banco de dados
            
        Raises:
            PostgreSQLExtractorError: Em caso de erro
        """
        try:
            logger.info(f"Gerando resumo do banco de dados (schema '{schema}')")
            
            all_metadata = self.get_all_tables_metadata(schema)
            relationships_df = self.get_table_relationships(schema)
            
            total_columns = sum(meta['column_count'] for meta in all_metadata.values())
            total_fks = sum(meta['foreign_key_count'] for meta in all_metadata.values())
            
            # Encontrar tabela com mais colunas
            max_columns_table = max(all_metadata.items(), 
                                  key=lambda x: x[1]['column_count'],
                                  default=("N/A", {"column_count": 0}))
            
            summary = {
                "database": self.config['database'],
                "schema": schema,
                "total_tables": len(all_metadata),
                "total_columns": total_columns,
                "total_relationships": len(relationships_df),
                "total_foreign_keys": total_fks,
                "table_with_most_columns": {
                    "name": max_columns_table[0],
                    "column_count": max_columns_table[1]['column_count']
                },
                "tables": list(all_metadata.keys())
            }
            
            logger.info(f"Resumo: {summary['total_tables']} tabelas, "
                       f"{summary['total_columns']} colunas, "
                       f"{summary['total_relationships']} relacionamentos")
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao gerar resumo do banco: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def main():
    """Exemplo de uso do extrator PostgreSQL"""
    print("🔍 PostgreSQL Metadata Extractor")
    print("=" * 50)
    
    try:
        # Inicializar extrator
        extractor = PostgreSQLExtractor()
        
        # Obter resumo do banco
        print("\n📊 Gerando resumo do banco de dados...")
        summary = extractor.get_database_summary()
        
        print(f"\nBanco: {summary['database']}")
        print(f"Schema: {summary['schema']}")
        print(f"Total de tabelas: {summary['total_tables']}")
        print(f"Total de colunas: {summary['total_columns']}")
        print(f"Total de relacionamentos: {summary['total_relationships']}")
        print(f"\nTabela com mais colunas: {summary['table_with_most_columns']['name']} "
              f"({summary['table_with_most_columns']['column_count']} colunas)")
        
        # Listar tabelas
        print(f"\n📋 Tabelas encontradas:")
        for i, table_name in enumerate(summary['tables'], 1):
            print(f"  {i}. {table_name}")
        
        # Exemplo: metadados de uma tabela específica
        if summary['tables']:
            first_table = summary['tables'][0]
            print(f"\n🔎 Metadados detalhados da tabela '{first_table}':")
            metadata = extractor.get_table_metadata(first_table)
            print(f"  Colunas: {metadata['column_count']}")
            print(f"  Chave primária: {metadata['primary_keys']}")
            print(f"  Chaves estrangeiras: {metadata['foreign_key_count']}")
        
        # Fechar conexão
        extractor.disconnect()
        print("\n✅ Extração concluída com sucesso!")
        
    except PostgreSQLExtractorError as e:
        print(f"\n❌ Erro: {e}")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")


if __name__ == "__main__":
    main()
