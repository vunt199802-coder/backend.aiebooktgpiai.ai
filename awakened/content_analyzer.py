from pinecone import Pinecone
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Optional
import json
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PineconeContentAnalyzer:
    def __init__(self):
        self.pc = Pinecone(api_key="b7a7f3f3-1962-4339-b889-31f73854b661")
        self.host = "ebooks-store-zaudryq.svc.aped-4627-b74a.pinecone.io"
        self.namespace = "ebooks-store-b7a7f3f3"
        self.index = self.pc.Index(host=f"https://{self.host}")

    def verify_index(self):
        """Verify index connection and contents"""
        try:
            stats = self.index.describe_index_stats()
            logger.info("\n=== Index Status ===")
            logger.info(f"Total vectors: {stats.total_vector_count}")
            logger.info(f"Dimension: {stats.dimension}")
            logger.info("Namespaces:")
            for name, ns_stats in stats.namespaces.items():
                logger.info(f"- {name or 'default'}: {ns_stats.vector_count} vectors")
            return True
        except Exception as e:
            logger.error(f"Index verification failed: {str(e)}")
            return False

    def analyze_file_content(self, file_id: str = None):
        """Analyze content of a specific file or all files"""
        try:
            # Get vectors
            sample_vector = np.random.rand(1536).tolist()
            filter_dict = {"file_id": file_id} if file_id else None
            
            results = self.index.query(
                vector=sample_vector,
                top_k=1000,  # Get a good sample
                namespace=self.namespace,
                include_metadata=True,
                filter=filter_dict
            )

            # Analyze content
            content_analysis = {}
            for match in results.matches:
                if not match.metadata or 'file_id' not in match.metadata:
                    continue

                current_file_id = match.metadata['file_id']
                if current_file_id not in content_analysis:
                    content_analysis[current_file_id] = {
                        'pages': set(),
                        'languages': set(),
                        'text_samples': [],
                        'total_chunks': 0
                    }

                # Track page numbers
                if 'page_number' in match.metadata:
                    content_analysis[current_file_id]['pages'].add(match.metadata['page_number'])

                # Track text samples
                if 'text' in match.metadata:
                    text = match.metadata['text'].strip()
                    if text:
                        content_analysis[current_file_id]['text_samples'].append(text[:200])
                        content_analysis[current_file_id]['total_chunks'] += 1

                        # Detect language
                        if any(word in text.lower() for word in ['the', 'and', 'in', 'on']):
                            content_analysis[current_file_id]['languages'].add('English')
                        if any(word in text for word in ['adalah', 'dan', 'yang', 'untuk']):
                            content_analysis[current_file_id]['languages'].add('Malay')
                        if any('\u4e00' <= char <= '\u9fff' for char in text):
                            content_analysis[current_file_id]['languages'].add('Chinese')

            # Display results
            logger.info("\n=== Content Analysis ===")
            for file_id, analysis in content_analysis.items():
                logger.info(f"\nFile ID: {file_id}")
                logger.info(f"Total chunks: {analysis['total_chunks']}")
                logger.info(f"Pages found: {len(analysis['pages'])}")
                logger.info(f"Languages detected: {', '.join(analysis['languages'])}")
                logger.info("Content samples:")
                for i, sample in enumerate(analysis['text_samples'][:3], 1):
                    logger.info(f"{i}. {sample}...")

            return content_analysis

        except Exception as e:
            logger.error(f"Content analysis failed: {str(e)}")
            return {}

    def search_content(self, query: str, language: str = None):
        """Search for specific content with language filter"""
        try:
            # Get initial vectors
            sample_vector = np.random.rand(1536).tolist()
            results = self.index.query(
                vector=sample_vector,
                top_k=100,
                namespace=self.namespace,
                include_metadata=True
            )

            # Search and filter results
            matches = []
            for match in results.matches:
                if not match.metadata or 'text' not in match.metadata:
                    continue

                text = match.metadata['text'].lower()
                query_lower = query.lower()

                # Simple language detection
                is_language_match = True
                if language:
                    if language == 'malay' and not any(word in text for word in ['adalah', 'dan', 'yang', 'untuk']):
                        is_language_match = False
                    elif language == 'english' and not any(word in text for word in ['the', 'and', 'in', 'on']):
                        is_language_match = False
                    elif language == 'chinese' and not any('\u4e00' <= char <= '\u9fff' for char in text):
                        is_language_match = False

                if query_lower in text and is_language_match:
                    matches.append({
                        'file_id': match.metadata.get('file_id', 'unknown'),
                        'page': match.metadata.get('page_number', 'unknown'),
                        'text': match.metadata['text'],
                        'score': match.score
                    })

            logger.info(f"\n=== Search Results for '{query}' ===")
            if language:
                logger.info(f"Language filter: {language}")
            
            for idx, match in enumerate(matches[:10], 1):
                logger.info(f"\nMatch {idx}:")
                logger.info(f"File: {match['file_id']}")
                logger.info(f"Page: {match['page']}")
                logger.info(f"Score: {match['score']:.4f}")
                logger.info(f"Text: {match['text'][:200]}...")

            return matches

        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return []

def main():
    analyzer = PineconeContentAnalyzer()
    
    if not analyzer.verify_index():
        logger.error("Failed to verify index, exiting...")
        return

    while True:
        print("\nOptions:")
        print("1. Search content")
        print("2. Analyze file content")
        print("3. List all files")
        print("4. Exit")

        choice = input("\nChoose an option (1-4): ")

        if choice == '1':
            query = input("Enter search term: ")
            language = input("Filter by language (english/malay/chinese/none): ").lower()
            if language == 'none':
                language = None
            analyzer.search_content(query, language)
            
        elif choice == '2':
            file_id = input("Enter file ID (or press Enter for all files): ").strip()
            if not file_id:
                file_id = None
            analyzer.analyze_file_content(file_id)
            
        elif choice == '3':
            analyzer.analyze_file_content()
            
        elif choice == '4':
            break
            
        else:
            print("Invalid option, please try again.")

if __name__ == "__main__":
    main()