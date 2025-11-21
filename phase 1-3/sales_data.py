import pandas as pd # pandas로 numpy까지 같이 설치된다고 함.
import numpy as np # numpy(보통 행렬같은 연산에 많이 쓰임.)를 쓰는지는 모르겠지만...

# 1. sales_data.csv 파일 
sales_data = pd.read_csv("./sales_data.csv")

sales_data.head(5)
#sales_data.info()
