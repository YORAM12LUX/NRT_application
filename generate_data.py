import os
import time
from datetime import datetime
import random
import pandas as pd

DATA_DIR = "./data"
# 512 Mo ou 1 Go
SIZES = [512 * 1024 * 1024, 1024 * 1024 * 1024]  

def generate_file():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    size = random.choice(SIZES)
    num_rows = size // 100 

    df = pd.DataFrame({
        "col1": range(num_rows),
        "col2": [random.randint(0, 100) for _ in range(num_rows)]
    })

    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f"data_{timestamp}.csv")
    df.to_csv(file_path, index=False)
    print(f"[GEN] {num_rows} lignes écrites dans : {file_path}")

if __name__ == "__main__":
    while True:
        generate_file()
        time.sleep(10)

