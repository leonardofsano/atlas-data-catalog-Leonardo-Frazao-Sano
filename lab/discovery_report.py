#!/usr/bin/env python3
"""
Relatório de Descoberta de Dados
Tarefa 4: Geração de relatórios com estatísticas do catálogo
"""

import json
import csv
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from atlas_client import AtlasClient, AtlasClientError

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DiscoveryReportError(Exception):
    """Exceção customizada para erros do Discovery Report"""
    pass


class DiscoveryReport:
    """
    Gerador de relatórios de descoberta de dados do catálogo Atlas.
    
    Gera relatórios detalhados com estatísticas sobre entidades catalogadas,
    exportando em múltiplos formatos (JSON, CSV).
    
    Attributes:
        atlas_client (AtlasClient): Cliente para interação com Apache Atlas
        report_data (dict): Dados coletados para o relatório
    """
    
    def __init__(self, atlas_client: AtlasClient):
        """
        Inicializa o gerador de relatórios.
        
        Args:
            atlas_client: Instância do AtlasClient
        """
        self.atlas = atlas_client
        self.report_data = {}
        logger.info("DiscoveryReport inicializado")
    
    def collect_statistics(self) -> Dict[str, Any]:
        """
        Coleta estatísticas gerais do catálogo Atlas.
        
        Returns:
            Dict com estatísticas agregadas
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            logger.info("📊 Coletando estatísticas do catálogo...")
            
            # Buscar databases
            databases = self.atlas.search_entities("*", limit=1000, type_name="hive_db")
            db_entities = databases.get('entities', [])
            
            # Buscar tabelas
            tables = self.atlas.search_entities("*", limit=1000, type_name="hive_table")
            table_entities = tables.get('entities', [])
            
            # Buscar colunas
            columns = self.atlas.search_entities("*", limit=5000, type_name="hive_column")
            column_entities = columns.get('entities', [])
            
            # Buscar processos (linhagem)
            processes = self.atlas.search_entities("*", limit=1000, type_name="Process")
            process_entities = processes.get('entities', [])
            
            stats = {
                "total_databases": len(db_entities),
                "total_tables": len(table_entities),
                "total_columns": len(column_entities),
                "total_lineages": len(process_entities),
                "databases": db_entities,
                "tables": table_entities,
                "columns": column_entities,
                "processes": process_entities
            }
            
            logger.info(f"Estatísticas coletadas: {stats['total_databases']} DBs, "
                       f"{stats['total_tables']} tabelas, {stats['total_columns']} colunas")
            
            return stats
            
        except AtlasClientError as e:
            error_msg = f"Erro ao coletar estatísticas: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def analyze_tables(self, table_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analisa detalhes das tabelas catalogadas.
        
        Args:
            table_entities: Lista de entidades de tabelas do Atlas
            
        Returns:
            Dict com análises das tabelas
        """
        logger.info("🔍 Analisando tabelas...")
        
        tables_info = []
        column_counts = {}
        
        for table in table_entities:
            guid = table.get('guid')
            display_text = table.get('displayText', 'N/A')
            
            # Tentar obter detalhes completos
            try:
                entity_detail = self.atlas.get_entity(guid)
                attributes = entity_detail.get('entity', {}).get('attributes', {})
                
                # Extrair informações
                qualified_name = attributes.get('qualifiedName', 'N/A')
                owner = attributes.get('owner', 'N/A')
                description = attributes.get('description', '')
                
                # Obter contagem de colunas dos parâmetros
                params = attributes.get('parameters', {})
                column_count = int(params.get('column_count', 0))
                
                table_info = {
                    "guid": guid,
                    "name": display_text,
                    "qualified_name": qualified_name,
                    "owner": owner,
                    "description": description,
                    "column_count": column_count
                }
                
                tables_info.append(table_info)
                column_counts[display_text] = column_count
                
            except Exception as e:
                logger.warning(f"Erro ao obter detalhes da tabela {display_text}: {e}")
                continue
        
        # Encontrar tabelas com mais colunas
        if column_counts:
            max_columns_table = max(column_counts.items(), key=lambda x: x[1])
            min_columns_table = min(column_counts.items(), key=lambda x: x[1])
            avg_columns = sum(column_counts.values()) / len(column_counts)
        else:
            max_columns_table = ("N/A", 0)
            min_columns_table = ("N/A", 0)
            avg_columns = 0
        
        analysis = {
            "tables_details": tables_info,
            "table_with_most_columns": {
                "name": max_columns_table[0],
                "column_count": max_columns_table[1]
            },
            "table_with_least_columns": {
                "name": min_columns_table[0],
                "column_count": min_columns_table[1]
            },
            "average_columns_per_table": round(avg_columns, 2)
        }
        
        logger.info(f"Análise concluída: tabela com mais colunas = '{max_columns_table[0]}' ({max_columns_table[1]})")
        
        return analysis
    
    def analyze_relationships(self, process_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analisa relacionamentos/linhagem entre tabelas.
        
        Args:
            process_entities: Lista de entidades de processo (linhagem)
            
        Returns:
            Dict com análise de relacionamentos
        """
        logger.info("🔗 Analisando relacionamentos...")
        
        relationships = []
        
        for process in process_entities:
            guid = process.get('guid')
            display_text = process.get('displayText', 'N/A')
            
            try:
                # Obter linhagem
                lineage = self.atlas.get_lineage(guid)
                
                # Extrair relações
                relations = lineage.get('relations', [])
                
                relationship_info = {
                    "process_name": display_text,
                    "guid": guid,
                    "relation_count": len(relations)
                }
                
                relationships.append(relationship_info)
                
            except Exception as e:
                logger.warning(f"Erro ao obter linhagem do processo {display_text}: {e}")
                continue
        
        analysis = {
            "total_relationships": len(relationships),
            "relationships_details": relationships
        }
        
        logger.info(f"Análise de relacionamentos concluída: {len(relationships)} processos")
        
        return analysis
    
    def generate_report_data(self) -> Dict[str, Any]:
        """
        Gera dados completos do relatório.
        
        Returns:
            Dict com todos os dados do relatório
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            logger.info("=" * 60)
            logger.info("📋 GERANDO RELATÓRIO DE DESCOBERTA")
            logger.info("=" * 60)
            
            # Coletar estatísticas
            stats = self.collect_statistics()
            
            # Analisar tabelas
            tables_analysis = self.analyze_tables(stats['tables'])
            
            # Analisar relacionamentos
            relationships_analysis = self.analyze_relationships(stats['processes'])
            
            # Compilar relatório
            report = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "generated_by": "DiscoveryReport",
                    "atlas_url": self.atlas.url
                },
                "summary": {
                    "total_databases": stats['total_databases'],
                    "total_tables": stats['total_tables'],
                    "total_columns": stats['total_columns'],
                    "total_lineages": stats['total_lineages'],
                    "average_columns_per_table": tables_analysis['average_columns_per_table']
                },
                "tables": {
                    "count": stats['total_tables'],
                    "table_with_most_columns": tables_analysis['table_with_most_columns'],
                    "table_with_least_columns": tables_analysis['table_with_least_columns'],
                    "details": tables_analysis['tables_details']
                },
                "relationships": {
                    "count": relationships_analysis['total_relationships'],
                    "details": relationships_analysis['relationships_details']
                },
                "databases": [
                    {
                        "guid": db.get('guid'),
                        "name": db.get('displayText', 'N/A')
                    }
                    for db in stats['databases']
                ]
            }
            
            self.report_data = report
            
            logger.info("✅ Relatório gerado com sucesso")
            logger.info("=" * 60)
            
            return report
            
        except Exception as e:
            error_msg = f"Erro ao gerar relatório: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def export_json(self, output_path: str = "discovery_report.json") -> str:
        """
        Exporta relatório em formato JSON.
        
        Args:
            output_path: Caminho do arquivo de saída
            
        Returns:
            Caminho do arquivo gerado
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            if not self.report_data:
                self.generate_report_data()
            
            # Criar diretório se não existir
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Escrever JSON
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.report_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📄 Relatório JSON exportado: {output_file}")
            return str(output_file)
            
        except Exception as e:
            error_msg = f"Erro ao exportar JSON: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def export_csv(self, output_path: str = "discovery_report.csv") -> str:
        """
        Exporta relatório de tabelas em formato CSV.
        
        Args:
            output_path: Caminho do arquivo de saída
            
        Returns:
            Caminho do arquivo gerado
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            if not self.report_data:
                self.generate_report_data()
            
            # Criar diretório se não existir
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Preparar dados para CSV (tabelas)
            tables_data = self.report_data.get('tables', {}).get('details', [])
            
            if not tables_data:
                logger.warning("Nenhum dado de tabelas para exportar em CSV")
                return str(output_file)
            
            # Escrever CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['name', 'qualified_name', 'owner', 'column_count', 'description', 'guid']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                writer.writeheader()
                for table in tables_data:
                    writer.writerow({
                        'name': table.get('name', ''),
                        'qualified_name': table.get('qualified_name', ''),
                        'owner': table.get('owner', ''),
                        'column_count': table.get('column_count', 0),
                        'description': table.get('description', ''),
                        'guid': table.get('guid', '')
                    })
            
            logger.info(f"📊 Relatório CSV exportado: {output_file}")
            return str(output_file)
            
        except Exception as e:
            error_msg = f"Erro ao exportar CSV: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def export_relationships_csv(self, output_path: str = "relationships_report.csv") -> str:
        """
        Exporta relatório de relacionamentos em formato CSV.
        
        Args:
            output_path: Caminho do arquivo de saída
            
        Returns:
            Caminho do arquivo gerado
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            if not self.report_data:
                self.generate_report_data()
            
            # Criar diretório se não existir
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Preparar dados para CSV (relacionamentos)
            relationships_data = self.report_data.get('relationships', {}).get('details', [])
            
            if not relationships_data:
                logger.warning("Nenhum dado de relacionamentos para exportar em CSV")
                return str(output_file)
            
            # Escrever CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['process_name', 'guid', 'relation_count']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                writer.writeheader()
                for relationship in relationships_data:
                    writer.writerow({
                        'process_name': relationship.get('process_name', ''),
                        'guid': relationship.get('guid', ''),
                        'relation_count': relationship.get('relation_count', 0)
                    })
            
            logger.info(f"🔗 Relatório de relacionamentos CSV exportado: {output_file}")
            return str(output_file)
            
        except Exception as e:
            error_msg = f"Erro ao exportar CSV de relacionamentos: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def generate_report(self, output_prefix: str = "discovery_report") -> Dict[str, str]:
        """
        Gera relatório completo em todos os formatos (JSON e CSV).
        
        Args:
            output_prefix: Prefixo para os arquivos de saída
            
        Returns:
            Dict com caminhos dos arquivos gerados
            
        Raises:
            DiscoveryReportError: Em caso de erro
        """
        try:
            logger.info("=" * 60)
            logger.info("📝 GERANDO RELATÓRIO COMPLETO")
            logger.info("=" * 60)
            
            # Gerar dados do relatório
            self.generate_report_data()
            
            # Exportar em múltiplos formatos
            json_file = self.export_json(f"{output_prefix}.json")
            csv_file = self.export_csv(f"{output_prefix}_tables.csv")
            relationships_file = self.export_relationships_csv(f"{output_prefix}_relationships.csv")
            
            files = {
                "json": json_file,
                "csv_tables": csv_file,
                "csv_relationships": relationships_file
            }
            
            # Log do resumo
            logger.info("\n" + "=" * 60)
            logger.info("✅ RELATÓRIO GERADO COM SUCESSO")
            logger.info("=" * 60)
            logger.info(f"Arquivos gerados:")
            logger.info(f"  - JSON: {json_file}")
            logger.info(f"  - CSV (Tabelas): {csv_file}")
            logger.info(f"  - CSV (Relacionamentos): {relationships_file}")
            logger.info("=" * 60)
            
            return files
            
        except Exception as e:
            error_msg = f"Erro ao gerar relatório completo: {e}"
            logger.error(error_msg)
            raise DiscoveryReportError(error_msg) from e
    
    def print_summary(self) -> None:
        """Imprime resumo do relatório no console."""
        if not self.report_data:
            self.generate_report_data()
        
        summary = self.report_data.get('summary', {})
        tables_info = self.report_data.get('tables', {})
        
        print("\n" + "=" * 60)
        print("📊 RESUMO DO CATÁLOGO DE DADOS")
        print("=" * 60)
        print(f"\n📂 Databases: {summary.get('total_databases', 0)}")
        print(f"📋 Tabelas: {summary.get('total_tables', 0)}")
        print(f"📝 Colunas: {summary.get('total_columns', 0)}")
        print(f"🔗 Linhagens: {summary.get('total_lineages', 0)}")
        print(f"📊 Média de colunas por tabela: {summary.get('average_columns_per_table', 0)}")
        
        print(f"\n🏆 Tabela com mais colunas:")
        most_cols = tables_info.get('table_with_most_columns', {})
        print(f"   {most_cols.get('name', 'N/A')} ({most_cols.get('column_count', 0)} colunas)")
        
        print(f"\n📉 Tabela com menos colunas:")
        least_cols = tables_info.get('table_with_least_columns', {})
        print(f"   {least_cols.get('name', 'N/A')} ({least_cols.get('column_count', 0)} colunas)")
        
        print("\n" + "=" * 60)


def main():
    """Exemplo de uso do gerador de relatórios"""
    from config import ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD
    
    print("📋 Discovery Report Generator")
    print("=" * 60)
    
    try:
        # Inicializar cliente Atlas
        print("\n🔧 Inicializando AtlasClient...")
        atlas = AtlasClient(ATLAS_URL, ATLAS_USER, ATLAS_PASSWORD)
        print("✅ AtlasClient inicializado")
        
        # Criar gerador de relatórios
        report_gen = DiscoveryReport(atlas)
        
        # Gerar relatório completo
        print("\n📝 Gerando relatório de descoberta...")
        files = report_gen.generate_report("discovery_report")
        
        # Imprimir resumo
        report_gen.print_summary()
        
        print("\n✅ Relatório gerado com sucesso!")
        print(f"\nArquivos criados:")
        for file_type, file_path in files.items():
            print(f"  - {file_type}: {file_path}")
        
    except DiscoveryReportError as e:
        print(f"\n❌ Erro no gerador de relatórios: {e}")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")


if __name__ == "__main__":
    main()
