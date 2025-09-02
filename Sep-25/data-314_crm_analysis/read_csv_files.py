import pandas as pd
import os
import glob

# data 폴더 경로 설정
data_folder = 'data'

# data 폴더 내의 모든 CSV 파일 찾기
csv_files = glob.glob(os.path.join(data_folder, '*.csv'))

print(f"발견된 CSV 파일 수: {len(csv_files)}")
print("파일 목록:")
for file in csv_files:
    print(f"  - {os.path.basename(file)}")

# 모든 CSV 파일을 읽어서 딕셔너리에 저장
dataframes = {}

for csv_file in csv_files:
    filename = os.path.basename(csv_file)
    try:
        # CSV 파일 읽기 (인코딩 문제가 있을 수 있으므로 utf-8로 시도)
        df = pd.read_csv(csv_file, encoding='utf-8')
        dataframes[filename] = df
        print(f"\n{filename} 읽기 완료:")
        print(f"  - 행 수: {len(df)}")
        print(f"  - 열 수: {len(df.columns)}")
        print(f"  - 열 이름: {list(df.columns)}")
    except UnicodeDecodeError:
        # utf-8로 읽기 실패시 cp949로 시도
        try:
            df = pd.read_csv(csv_file, encoding='cp949')
            dataframes[filename] = df
            print(f"\n{filename} 읽기 완료 (cp949 인코딩):")
            print(f"  - 행 수: {len(df)}")
            print(f"  - 열 수: {len(df.columns)}")
            print(f"  - 열 이름: {list(df.columns)}")
        except Exception as e:
            print(f"\n{filename} 읽기 실패: {e}")

print(f"\n총 {len(dataframes)}개 파일을 성공적으로 읽었습니다.")

# 첫 번째 파일의 처음 몇 행 확인
if dataframes:
    first_file = list(dataframes.keys())[0]
    print(f"\n{first_file}의 처음 5행:")
    print(dataframes[first_file].head())

# 모든 데이터프레임의 기본 정보 출력
print("\n" + "="*50)
print("모든 파일의 기본 정보:")
print("="*50)
for filename, df in dataframes.items():
    print(f"\n{filename}:")
    print(f"  - 크기: {df.shape}")
    print(f"  - 데이터 타입:")
    for col, dtype in df.dtypes.items():
        print(f"    {col}: {dtype}")
    print(f"  - 결측값:")
    print(df.isnull().sum())
