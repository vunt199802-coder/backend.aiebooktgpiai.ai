from pinecone import Pinecone
import numpy as np
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PineconeManager:
    def __init__(self):
        self.api_key = "b7a7f3f3-1962-4339-b889-31f73854b661"
        self.pc = Pinecone(api_key=self.api_key)
        self.index_name = "ebooks-store"
        self.host = "ebooks-store-zaudryq.svc.aped-4627-b74a.pinecone.io"
        self.namespace = "ebooks-store-b7a7f3f3"
        self.dimension = 1536
        self.index = None
        self.connect()

    def connect(self):
        """Establish connection to the index"""
        self.index = self.pc.Index(host=f"https://{self.host}")
        
    def get_namespace_stats(self) -> Dict:
        """Get detailed statistics for each namespace"""
        stats = self.index.describe_index_stats()
        return {
            "total_vectors": stats.get('total_vector_count', 0),
            "dimension": stats.get('dimension', 0),
            "index_fullness": stats.get('index_fullness', 0),
            "namespaces": stats.get('namespaces', {})
        }

    async def search_vectors(self, query_vector: List[float], top_k: int = 5, 
                           filter_dict: Optional[Dict] = None) -> Dict:
        """Search for similar vectors with optional filtering"""
        return self.index.query(
            vector=query_vector,
            top_k=top_k,
            namespace=self.namespace,
            include_metadata=True,
            filter=filter_dict
        )

    def delete_by_filter(self, filter_dict: Dict) -> bool:
        """Delete vectors matching specific criteria"""
        try:
            self.index.delete(
                namespace=self.namespace,
                filter=filter_dict
            )
            return True
        except Exception as e:
            logger.error(f"Error deleting vectors: {str(e)}")
            return False

    def cleanup_namespace(self, namespace: str) -> bool:
        """Clean up a specific namespace"""
        try:
            self.index.delete(
                delete_all=True,
                namespace=namespace
            )
            return True
        except Exception as e:
            logger.error(f"Error cleaning namespace {namespace}: {str(e)}")
            return False

    def display_namespace_contents(self, sample_size: int = 5):
        """Display sample contents from each namespace"""
        stats = self.get_namespace_stats()
        
        logger.info("\n=== Namespace Contents ===")
        for ns_name, ns_stats in stats['namespaces'].items():
            logger.info(f"\nNamespace: {ns_name}")
            logger.info(f"Vector count: {ns_stats['vector_count']}")
            
            if ns_stats['vector_count'] > 0:
                # Get a sample vector to query with
                random_vector = np.random.rand(self.dimension).tolist()
                sample_results = self.index.query(
                    vector=random_vector,
                    top_k=sample_size,
                    namespace=ns_name,
                    include_metadata=True
                )
                
                logger.info("\nSample vectors:")
                for idx, match in enumerate(sample_results['matches'], 1):
                    logger.info(f"\nSample {idx}:")
                    logger.info(f"ID: {match['id']}")
                    logger.info(f"Score: {match['score']}")
                    logger.info(f"Metadata: {match['metadata']}")

    def run_diagnostics(self):
        """Run comprehensive diagnostics on the index"""
        try:
            logger.info("\n=== Running Index Diagnostics ===")
            
            # Get basic stats
            stats = self.get_namespace_stats()
            logger.info("\nBasic Statistics:")
            logger.info(f"Total vectors: {stats['total_vectors']}")
            logger.info(f"Dimension: {stats['dimension']}")
            logger.info(f"Index fullness: {stats['index_fullness']*100:.2f}%")
            
            # Namespace analysis
            logger.info("\nNamespace Analysis:")
            for ns_name, ns_stats in stats['namespaces'].items():
                logger.info(f"\nNamespace: {ns_name or 'default'}")
                logger.info(f"Vector count: {ns_stats['vector_count']}")
                
                # Calculate percentage of total vectors
                percentage = (ns_stats['vector_count'] / stats['total_vectors'] * 100) if stats['total_vectors'] > 0 else 0
                logger.info(f"Percentage of total: {percentage:.2f}%")
            
            return True
            
        except Exception as e:
            logger.error(f"Diagnostics failed: {str(e)}")
            return False

def main():
    try:
        manager = PineconeManager()
        
        # Run diagnostics
        manager.run_diagnostics()
        
        # Display sample contents
        manager.display_namespace_contents()
        
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()