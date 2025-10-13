from dataclasses import dataclass, field
from datetime import datetime, timedelta
import random
from typing import List, Optional, Dict, Tuple

@dataclass
class FileMetadata:
    """Represents metadata for a file in the distributed storage system"""
    filename: str
    owner: str
    timestamp: datetime
    tags: List[str]
    permissions: str
    file_size: int
    file_id: str = field(default_factory=lambda: str(random.randint(100000, 999999)))
    
    def __lt__(self, other):
        return self.filename < other.filename
    
    def __eq__(self, other):
        return self.filename == other.filename
    
    def __hash__(self):
        return hash(self.filename)
    
    def __repr__(self):
        return f"FileMetadata({self.filename})"
    

class MetadataGenerator:
    """Generate realistic metadata for testing"""
    
    OWNERS = ["alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry"]
    TAGS = ["work", "personal", "project", "backup", "archive", "shared", 
            "important", "draft", "final", "review", "public", "private"]
    EXTENSIONS = [".txt", ".pdf", ".docx", ".jpg", ".png", ".mp4", ".zip", ".py", ".java", ".cpp"]
    
    @staticmethod
    def generate_metadata(count=10000):
        """Generate random metadata entries"""
        metadata_list = []
        base_time = datetime.now()
        
        for i in range(count):
            filename = f"file_{i:05d}{random.choice(MetadataGenerator.EXTENSIONS)}"
            owner = random.choice(MetadataGenerator.OWNERS)
            timestamp = base_time - timedelta(days=random.randint(0, 365))
            tags = random.sample(MetadataGenerator.TAGS, k=random.randint(1, 4))
            permissions = random.choice(["rw", "r", "rwx"])
            file_size = random.randint(1024, 10485760)  # 1KB to 10MB
            
            metadata_list.append(FileMetadata(
                filename=filename,
                owner=owner,
                timestamp=timestamp,
                tags=tags,
                permissions=permissions,
                file_size=file_size
            ))
        
        return metadata_list
