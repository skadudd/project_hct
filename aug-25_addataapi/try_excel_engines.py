import pandas as pd
import os

def try_read_excel_with_different_engines():
    """여러 엔진을 시도하여 엑셀 파일을 읽습니다."""
    
    excel_file = '20250701_20250818.xlsx'
    print(f"읽을 파일: {excel_file}")
    print(f"파일 크기: {os.path.getsize(excel_file)} bytes")
    
    # 시도할 엔진들
    engines = ['openpyxl', 'xlrd', 'odf']
    
    for engine in engines:
        print(f"\n=== {engine} 엔진으로 시도 ===")
        try:
            # 엔진이 설치되어 있는지 확인
            if engine == 'openpyxl':
                import openpyxl
                print("openpyxl 설치됨")
            elif engine == 'xlrd':
                import xlrd
                print("xlrd 설치됨")
            elif engine == 'odf':
                import odf
                print("odf 설치됨")
            
            # 시트 이름 확인
            xl = pd.ExcelFile(excel_file, engine=engine)
            print(f"시트 이름: {xl.sheet_names}")
            
            # 첫 번째 시트 읽기
            df = pd.read_excel(excel_file, sheet_name=0, engine=engine)
            print(f"성공! DataFrame 크기: {df.shape}")
            print(f"컬럼명: {list(df.columns)}")
            print("\n처음 5행:")
            print(df.head())
            
            return df, engine
            
        except ImportError as e:
            print(f"{engine} 엔진이 설치되지 않음: {e}")
        except Exception as e:
            print(f"{engine} 엔진으로 읽기 실패: {e}")
    
    print("\n모든 엔진으로 읽기 실패")
    return None, None

if __name__ == "__main__":
    df, successful_engine = try_read_excel_with_different_engines()
    
    if df is not None:
        print(f"\n🎉 {successful_engine} 엔진으로 성공적으로 읽었습니다!")
        print(f"총 {len(df)} 행, {len(df.columns)} 컬럼이 있습니다.")
    else:
        print("\n❌ 모든 엔진으로 읽기 실패했습니다.")

