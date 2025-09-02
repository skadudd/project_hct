import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')

def read_excel_to_dataframe(file_path='20250701_20250818.xlsx'):
    """
    엑셀 파일을 읽어서 DataFrame으로 변환하는 함수
    
    Parameters:
    file_path (str): 엑셀 파일 경로
    
    Returns:
    pd.DataFrame: 읽어온 데이터프레임
    """
    
    print(f"📁 파일 읽기 시도: {file_path}")
    
    # 파일 존재 확인
    if not os.path.exists(file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
        return None
    
    # 파일 크기 확인
    file_size = os.path.getsize(file_path)
    print(f"📊 파일 크기: {file_size:,} bytes")
    
    # 시도할 엔진들 (우선순위 순)
    engines = ['openpyxl', 'xlrd', 'pyxlsb']
    
    for engine in engines:
        print(f"\n🔧 {engine} 엔진으로 시도 중...")
        
        try:
            # 엔진별 시트 확인
            try:
                xl = pd.ExcelFile(file_path, engine=engine)
                print(f"📋 발견된 시트: {xl.sheet_names}")
                sheet_names = xl.sheet_names
            except Exception as e:
                print(f"⚠️ 시트 정보 확인 실패: {e}")
                continue
            
            # 첫 번째 시트 읽기
            df = pd.read_excel(file_path, sheet_name=0, engine=engine)
            
            print(f"✅ 성공! {engine} 엔진으로 읽기 완료")
            print(f"📏 DataFrame 크기: {df.shape[0]}행 × {df.shape[1]}열")
            print(f"📝 컬럼명: {list(df.columns)}")
            
            # 데이터 미리보기
            print(f"\n📊 데이터 미리보기 (처음 5행):")
            print(df.head())
            
            # 기본 정보
            print(f"\n📈 데이터 정보:")
            df.info()
            
            return df
            
        except ImportError:
            print(f"❌ {engine} 엔진이 설치되지 않음")
            continue
        except Exception as e:
            print(f"❌ {engine} 엔진 실패: {e}")
            continue
    
    # 모든 엔진 실패 시 대안 시도
    print(f"\n🔄 대안 방법들 시도 중...")
    
    # CSV로 시도 (파일이 실제로는 CSV일 경우)
    try:
        print("📄 CSV 형식으로 시도...")
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"✅ CSV로 읽기 성공! 크기: {df.shape}")
        return df
    except:
        try:
            df = pd.read_csv(file_path, encoding='cp949')
            print(f"✅ CSV(cp949)로 읽기 성공! 크기: {df.shape}")
            return df
        except:
            pass
    
    # 탭 구분자로 시도
    try:
        print("📄 탭 구분자로 시도...")
        df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
        print(f"✅ 탭 구분자로 읽기 성공! 크기: {df.shape}")
        return df
    except:
        pass
    
    print("❌ 모든 방법으로 파일 읽기 실패")
    return None

def read_all_sheets(file_path='20250701_20250818.xlsx'):
    """
    엑셀 파일의 모든 시트를 읽어서 딕셔너리로 반환
    
    Returns:
    dict: {시트명: DataFrame} 형태의 딕셔너리
    """
    
    engines = ['openpyxl', 'xlrd']
    
    for engine in engines:
        try:
            # 모든 시트 읽기
            all_sheets = pd.read_excel(file_path, sheet_name=None, engine=engine)
            
            print(f"✅ 모든 시트 읽기 성공! 총 {len(all_sheets)}개 시트")
            
            for sheet_name, df in all_sheets.items():
                print(f"📋 {sheet_name}: {df.shape[0]}행 × {df.shape[1]}열")
            
            return all_sheets
            
        except Exception as e:
            print(f"❌ {engine} 엔진으로 모든 시트 읽기 실패: {e}")
            continue
    
    return None

if __name__ == "__main__":
    print("🚀 엑셀 파일 읽기 시작")
    print("=" * 50)
    
    # 단일 시트 읽기
    df = read_excel_to_dataframe()
    
    if df is not None:
        print(f"\n🎉 DataFrame 생성 완료!")
        print(f"✨ 변수명 'df'로 사용 가능")
        print(f"📊 기본 통계:")
        print(df.describe())
        
        # 결측값 확인
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            print(f"\n⚠️ 결측값 정보:")
            print(null_counts[null_counts > 0])
        else:
            print(f"\n✅ 결측값 없음")
    
    else:
        print(f"\n❌ DataFrame 생성 실패")
        print(f"💡 파일이 손상되었거나 지원되지 않는 형식일 수 있습니다.")
        
        # 파일 형식 추가 분석
        try:
            with open('20250701_20250818.xlsx', 'rb') as f:
                header = f.read(20)
                print(f"📋 파일 헤더: {header}")
                
                if header.startswith(b'PK'):
                    print("💡 ZIP 기반 파일 (정상적인 .xlsx 파일)")
                elif header.startswith(b'\xd0\xcf\x11\xe0'):
                    print("💡 OLE2 기반 파일 (.xls 파일)")
                else:
                    print("💡 알 수 없는 파일 형식")
                    
        except Exception as e:
            print(f"❌ 파일 분석 실패: {e}")

