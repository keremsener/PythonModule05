import abc
import typing


class DataProcessor(abc.ABC): # Ana classımız
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.counter: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("Processor storage is empty.")

        return self.storage.pop(0)


class NumericProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) in (int, float): # int veya floatsa direkt true döndür
            return True

        if type(data) is list: # listse
            if len(data) == 0: # boşsa false döndür
                return False
            return all(type(item) in (int, float) for item in data) # Liste boş değilse, içindeki BÜTÜN elemanların teker teker int veya float olup OLMADIĞINI KONTROL ET
        return False # hiçbiri değilse de false döndür

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data): # boş dönmüşse yani yukardan False dönmüşse hata gönder
            raise ValueError("Improper numeric data")
        if type(data) is list: # eğerki list dönmüşse
            for item in data:
                self.storage.append((self.counter, str(item))) # tüm dataları str'ye çevir sıraya ekle
                self.counter += 1 # sayacı 1 artır
        else:
            self.storage.append((self.counter, str(data))) # liste değilse direkt data'yı str'ye çevir ve listeye ekle
            self.counter += 1 # sayacı 1 artır


class TextProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) is str: # türü string ise true döndür işi bitir
            return True

        if type(data) is list: # listeyse
            if len(data) == 0: # boşsa false döndür
                return False
            return all(type(item) is str for item in data) # Liste boş değilse, içindeki BÜTÜN elemanların teker teker str olup OLMADIĞINI KONTROL ET.

        return False # hiçbiri değilse false döndür

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data") # yukardan False dönmüşse hata fırlat

        if type(data) is list: # listse
            list_data = typing.cast(list[str], data) # Mypy müfettişine bu datanın kesinlikle bir list[str] olduğunu dikte et (Mypy'ı sustur).
            for item in list_data: # tüm list_data'yı sıraya ekle
                self.storage.append((self.counter, item))
                self.counter += 1 # sayacı 1 artır
        else:
            str_data = typing.cast(str, data) # list değilse tek başınadır
            self.storage.append((self.counter, str_data)) # direkt listeye ekle
            self.counter += 1 # sayacı 1 artır


class LogProcessor(DataProcessor):

    def validate(self, data: typing.Any) -> bool:
        if type(data) is dict: # dictinory ise
            if "log_level" in data and "log_message" in data: # data'da log level ve log_message varsa
                if (type(data["log_level"]) is str and # log level ve log message str ise
                        type(data["log_message"]) is str):
                    return True # true döndür
            return False # data'da log level veya log_message yoksa false döndür

        if type(data) is list: # list ise
            if len(data) == 0: # list ama boşsa
                return False # false döndür
    
            for item in data:
                if type(item) is not dict: # dict değilse false döndür
                    return False
                
                if "log_level" not in item or "log_message" not in item: # log level veya log_message yoksa
                    return False #  false döndür
                
                if type(item["log_level"]) is not str or type(item["log_message"]) is not str: # Bu anahtarlar var ama değerleri string değilse
                    return False # false döndür
            
            # Hiçbir sorun yoksa
            return True # True döndür
        return False # list değilse false döndür

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data): # yukarıdan false dönmüse
            raise ValueError("Improper log data") # hata fırlat

        if type(data) is list: # data listse
            list_data = typing.cast(list[dict[str, str]], data) # Mypy kısmasın diye list olduğunu doğrula

            for item in list_data: # list_data'daki tüm itemler için
                lvl = item['log_level'] # Sözlüğün içinden log_level değerini çek (örn: "ERROR")
                msg = item['log_message'] # Sözlüğün içinden mesajı çek (örn: "Sistem çöktü")
                formatted_log = f"{lvl}: {msg}" # İkisini araya ':' koyarak tek bir string yap ("ERROR: Sistem çöktü")
                self.storage.append((self.counter, formatted_log)) # sıraya ekle
                self.counter += 1 # sayacı 1 artır

        else:
            dict_data = typing.cast(dict[str, str], data)
            lvl = dict_data['log_level']
            msg = dict_data['log_message']
            formatted_log = f"{lvl}: {msg}"
            self.storage.append((self.counter, formatted_log))
            self.counter += 1


if __name__ == "__main__":
    print("=== Code Nexus Data Processor ===")

    # -------------------------------------------------------------------------
    # 1. NUMERIC PROCESSOR TESTLERİ
    # -------------------------------------------------------------------------
    print("Testing Numeric Processor.")
    num_proc = NumericProcessor()

    # Doğrulama testleri
    print(f"Trying to validate input '42': {num_proc.validate(42)}")
    print(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")

    # Geçerli sayı listesini sisteme yedirme
    numeric_data: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    num_proc.ingest(numeric_data)

    # FIFO sırasına göre ilk 3 elemanı depodan çekip ekrana basma
    print("Extracting 3 values...")
    for _ in 0, 1, 2:
        rank, val = num_proc.output()
        print(f"Numeric value {rank}:")
        print(val)

    # -------------------------------------------------------------------------
    # 2. TEXT PROCESSOR TESTLERİ
    # -------------------------------------------------------------------------
    print("Testing Text Processor")
    text_proc = TextProcessor()

    print(f"Trying to validate input '42': {text_proc.validate(42)}")

    text_data = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_data}")
    text_proc.ingest(text_data)

    print("Extracting 1 value...")
    t_rank, t_val = text_proc.output()
    print(f"Text value {t_rank}: {t_val}")

    # -------------------------------------------------------------------------
    # 3. LOG PROCESSOR TESTLERİ
    # -------------------------------------------------------------------------
    print("Testing Log Processor.")
    log_proc = LogProcessor()

    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")

    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!!'}
    ]
    print(f"Processing data: {log_data}")
    log_proc.ingest(log_data)

    print("Extracting 2 values...")
    for _ in 0, 1:
        l_rank, l_val = log_proc.output()
        print(f"Log entry {l_rank}: {l_val}")