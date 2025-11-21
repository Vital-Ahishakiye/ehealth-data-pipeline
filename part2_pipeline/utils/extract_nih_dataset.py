"""
Extract NIH Chest X-ray Dataset from HuggingFace
Downloads and prepares data for ETL pipeline
"""
import sys
import io
from pathlib import Path
import pandas as pd
from collections import Counter
from part2_pipeline.config import NIH_DATASET_NAME, NIH_DATASET_SIZE, DATA_DIR, NIH_SUBSET_NAME
from part2_pipeline.utils.logger import PipelineLogger
from datasets import load_dataset


# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from part2_pipeline.config import NIH_DATASET_NAME, NIH_DATASET_SIZE, DATA_DIR, NIH_SUBSET_NAME
from part2_pipeline.utils.logger import PipelineLogger

# Force UTF-8 output for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def download_nih_dataset():
    """Download NIH metadata CSV directly from HuggingFace and save a sampled subset"""
    with PipelineLogger("NIH Dataset Download") as logger:
        try:
            url = "https://huggingface.co/datasets/alkzar90/NIH-Chest-X-ray-dataset/resolve/main/data/Data_Entry_2017_v2020.csv"
            logger.info(f"Downloading NIH metadata CSV from: {url}")
            
            df = pd.read_csv(url)
            logger.info(f"Loaded {len(df):,} records")

            # Sample subset
            if len(df) > NIH_DATASET_SIZE:
                df = df.sample(n=NIH_DATASET_SIZE, random_state=42).reset_index(drop=True)
                logger.info(f"Sampled {NIH_DATASET_SIZE:,} records")

            # Save subset
            output_path = DATA_DIR / NIH_SUBSET_NAME
            df.to_csv(output_path, index=False)
            logger.info(f"✅ Dataset saved successfully at {output_path}")

            return output_path
        except Exception as e:
            logger.error(f"Failed to download dataset: {e}")
            raise


def load_nih_dataset():
    """Load NIH dataset from local CSV"""
    csv_path = DATA_DIR / NIH_SUBSET_NAME
    
    if not csv_path.exists():
        print(f"❌ Dataset not found at: {csv_path}")
        print("   Run download first: python extract_nih_dataset.py")
        return None
    
    print(f"📂 Loading dataset from: {csv_path}")
    dataset = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(dataset):,} records")
    return dataset


def main():
    """Main execution"""
    print("\n" + "=" * 60)
    print("📥 NIH Chest X-ray Dataset Extraction")
    print("=" * 60)
    
    try:
        output_path = download_nih_dataset()
        print(f"\n✅ SUCCESS! Dataset ready at: {output_path}")
        print("\n📋 Next steps:")
        print("   1. Run: python generate_synthetic_reports.py")
        print("   2. Run: python etl_pipeline.py")
        
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()