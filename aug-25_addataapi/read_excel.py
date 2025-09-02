import pandas as pd
import os

def read_excel_file():
    """현재 폴더의 엑셀 파일을 읽어서 DataFrame으로 생성합니다."""
    
    # 현재 폴더의 엑셀 파일 확인
    excel_files = [f for f in os.listdir('.') if f.endswith('.xlsx')]
    print(f"발견된 엑셀 파일: {excel_files}")
    
    if not excel_files:
        print("엑셀 파일을 찾을 수 없습니다.")
        return None
    
    # 첫 번째 엑셀 파일 사용
    excel_file = excel_files[0]
    print(f"읽을 파일: {excel_file}")
    
    try:
        # 엑셀 파일의 시트 이름 확인 (openpyxl 엔진 사용)
        xl = pd.ExcelFile(excel_file, engine='openpyxl')
        print(f"시트 이름: {xl.sheet_names}")
        
        # 첫 번째 시트를 DataFrame으로 읽기 (openpyxl 엔진 사용)
        df = pd.read_excel(excel_file, sheet_name=0, engine='openpyxl')
        
        print(f"\nDataFrame 크기: {df.shape}")
        print(f"컬럼명: {list(df.columns)}")
        
        print("\n처음 5행:")
        print(df.head())
        
        print("\nDataFrame 정보:")
        print(df.info())
        
        print("\n기본 통계 정보:")
        print(df.describe())
        
        return df
        
    except Exception as e:
        print(f"파일 읽기 중 오류 발생: {e}")
        return None

if __name__ == "__main__":
    # 엑셀 파일 읽기
    df = read_excel_file()
    
    if df is not None:
        print(f"\n성공적으로 DataFrame이 생성되었습니다!")
        print(f"총 {len(df)} 행, {len(df.columns)} 컬럼이 있습니다.")
