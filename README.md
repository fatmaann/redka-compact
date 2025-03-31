# Redka Talk

A key-value LSM database inspired by BitCask.

## How to use
1. Clone this repository using ```git clone --recursive```.
2. Make a build directory:
```bash
mkdir build
cd build
```
3. Invoke cmake
```bash
cmake ..
```
4. Build the app
```bash
make
```
5. Run the app 
```bash
./RedkaTalk
```

## Отчет по заданию

Redka is expected to be a fast mostly in-memory store, so all
files (SSTs and logs) are mmapped and generally expected to fit
in RAM. The architecture of a system is a basic LSM database
with strong influence of Riak's BitCask (see Petrov's book).

Все файлы mmaped и их содержимое обновляется напрямую в RAM. Для этого испольузется класс `MappedFile`, описанный в файлах `mapped_file`.

### 1. Запросы:


```
the main process redka-talk is a single-thread poll-based TCP server. It accepts queries in RDX or JDR, sends responses in the same format.

 3. the wire protocol for `redka-talk` is RDX or JDR. JDR uses
    newlines to separate different requests and responses.

     1. `b0b-123` (a reference) is a query of a specific object
        if sent by a client; an id of a newly created object
        if sent by the server;
     2. `{@b0b-123 name:"Alice" address:"Wonderland"}`
        (a PLEX object, in this case an Eulerian set) is either 
        a response if sent by the server or a write if sent by
        a client; writes may use versioning in the
        usual RDX way, e.g. `{@b0b-123 address@2:"Tea Party"}`
        New object writes have no id: {name:"Mad Hatter"}
     3. So, every query is responded with a PLEX object or 
        `RDKAnone`; every write is responded with the id of the
        edited/created object. Any unclear message is responded
        with `RDKAbad`; on a parsing error `RDXbad`, connection
        closed.
```

Готово: redka-talk реализован как асинхронный однопоточный poll-based TCP сервер на корутинах. Он принимает запросы в JDR и отвечает в том же формате. В качестве идентификаторов были выбраны UUID версии 4 для возможности в дальнейшем децентрализованно добавлять новые объекты без дополнительной синхронизации идентификаторов.

Фактически есть запросы трех видов:
1. Запрос на создание нового объекта (New object writes have no id: `{name:"Mad Hatter"}` --- в ответ получаем идентификатор)\
Вывод тестового клиента при отправке запросов такого вида:
```
Connected to the server
Message sent: {name:"Alice"}
Server response: 6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1
```
   
2. Запрос на обновление существующего объекта. (`{@b0b-123 name:"Alice" address:"Wonderland"}`; a write if sent by a client; writes may use versioning in the usual RDX way, e.g. `{@b0b-123 address@2:"Tea Party"}` --- в ответ получаем идентификатор)\
Вывод тестового клиента при отправке запросов такого вида:
```
Connected to the server
Message sent: {@6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1 surname:"Liddell" address:"Wonderland"}
Server response: 6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1
```
```
Connected to the server
Message sent: {@6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1 address@2:"Home"}
Server response: 6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1
```

3. Запрос на чтение определенного объекта по его идентификатору (`b0b-123` (a reference) is a query of a specific object if sent by a client --- в ответ получаем сам объект)\
Вывод тестового клиента при отправке запросов такого вида:
```
Connected to the server
Message sent: 6e88d1ce-ddd4-4a97-8e96-29a00adfc8a1
Server response: {address@2:"Home" name:"Alice" surname:"Liddell"}
```

Также, по согласованию, `RDKAnone`, `RDKAbad`; `RDXbad` выражаются числовыми кодами:
```
// Response codes, starting from 1: errors
const int RDKAnone = 0;
const int RDKAbad = 1;
const int RDXbad = 2;
```
Вывод тестового клиента при получении этих ответов:
```
Connected to the server
Message sent: 6e88d1ce-ddd4-4a97-8e96-29a00adfc8a2
Server response: 0
```
```
Connected to the server
Message sent: @6e88d1ce-ddd4-4a97-8e96-29a00adfc8a2
Server response: 1
```
```
        bool gotParseError = false;
        try {
            if (!parseMessage(message, idOrRecord, isRead, isUpdate, idOfRecordToUpdate)) {
                co_await socket.WriteAll(std::span(std::to_string(RDKAbad).c_str(), 1));
                break;
            }
        } catch (...) {
            gotParseError = true;
            // 'co_await' cannot be used in the handler of a try block
        }
        if (gotParseError) {
            co_await socket.WriteAll(std::span(std::to_string(RDXbad).c_str(), 1));
            break;
        }
```
Не все некорректные запросы (в основном с некорректным JDR форматом) получают ответы `RDKAbad` или `RDXbad`, так как сосредоточились на более важной части проекта и отставили какие-то подобные моменты.


###  2. WAL логика

```
      - the main process maintains the WAL (write ahead log)
        where new writes reside; it also keeps a hash table to
        find new writes in the log. The hash table is of the
        most basic kind, `{id128 -> u32[4]}` so up to four
        writes to the same object can be tracked. In case a 5th
        write arrives to the same object, `redka-talk` merges
        all five, puts the result into the log, updates the
        index. The log is RDX. 
```

Для ускорения чтения WAL хеш-таблица используется вида `{id128 -> (u32, u32)[4]}`, где хранится связь идентификатора к офсету записи и ее размер (чтобы можно было сразу скопировать всю запись за O(1)). После 4 записей корректно проходит их мердж и запись обновленной записи в лог, а также обновление хеш-таблицы. Для удобства чтения лога человеком и проверки его работы, а также чтобы не тратить время и силы на вопросы сериализации/десоциализации в RDX, лог ведется в том же формате JDR. Записи формируются в таком формате, чтобы в дальнейшем утилита rdx-cli могла корректно смерджить записи из WAL файла и получить те же результаты.\
Эта логика описана в функции `writeWALToFile`:
<details>
  <summary>код writeWALToFile</summary>
    
  ```C++
  void writeWALToFile(const std::string &logEntry, std::string const &recordId) {
    size_t newRecordOffset = wal_log.size();
    if (recordIdToOffset.find(recordId) == recordIdToOffset.end()) {
        appendToWAL(wal_log, logEntry);
        recordIdToOffset[recordId] = {std::make_pair(newRecordOffset, logEntry.size()),
                                      {-1u, 0}, {-1u, 0}, {-1u, 0}};
    } else {
        auto &recordMetadatas = recordIdToOffset[recordId];
        bool fourWritesAreTracked = true;
        for (auto &recordMetadata : recordMetadatas) {
            // no offset
            if (recordMetadata.first == -1) {
                recordMetadata.first = newRecordOffset;
                recordMetadata.second = logEntry.size();
                fourWritesAreTracked = false;
                appendToWAL(wal_log, logEntry);
                break;
            }
        }
        if (fourWritesAreTracked) {
            // Merge all four writes and add it
            std::string mergedRecord = logEntry;
            mergedRecord = mergeTwoRecords(mergedRecord, readFromWALFileById(recordId));
            std::stringstream new_record;
            new_record << "{@" << recordId << " " << mergedRecord << "}";
            recordIdToOffset[recordId] = {std::make_pair(newRecordOffset, new_record.str().size()),
                                          {-1u, 0}, {-1u, 0}, {-1u, 0}};
            appendToWAL(wal_log, new_record.str());
        }
    }
}
  ```
где `recordIdToOffset` это хеш-таблица. 
</details>
Для чтения из WAL используется функция `readFromWALFileById`, которая использует эту хеш-таблицу, как описано:
<details>
  <summary>код readFromWALFileById</summary>
    
  ```C++
std::string readFromWALFileByOffset(MappedFile &mmapFile, const size_t recordOffset, const size_t recordLength) {
    if (recordOffset >= mmapFile.size()) {
        // Handle error: offset beyond file size.
        return "";
    }
    char *recordStart = mmapFile.data() + recordOffset;
    return std::string(recordStart, recordLength);
}

std::string readFromWALFileById(const std::string &recordId) {
    std::string mergedRecord;
    auto recordsMetadata = recordIdToOffset[recordId];
    for (auto &recordMetadata : recordsMetadata) {
        if (recordMetadata.first == -1)
            break;

        auto previousLogEntry = readFromWALFileByOffset(wal_log, recordMetadata.first, recordMetadata.second);
        mergedRecord = mergeTwoRecords(mergedRecord, previousLogEntry);
    }
    return mergedRecord;
}
  ```
</details>


### 3. Логика компактизации

```
      - once a log chunk reaches the size limit, redka starts a new one; the total size of all
      chunks can not exceed 4GB. Older chunks are deleted once the data is moved to SST files. 
```

Готово: При превышении размера WAL (`MAX_WAL_SIZE`) все текущие записи из `recordIdToOffset` собираются в батч и сохраняются в SST-уровень `L0` через `db.flushBatchToL0()`.
После успешного сохранения WAL очищается (`truncate`) и хеш-таблица сбрасывается.



<details>
  <summary>Код из функции `writeWALToFile`</summary>
    
  ```C++
if (wal_log.size() > MAX_WAL_SIZE) {
        std::cout << "wal_log.size() > MAX_WAL_SIZE" << std::endl;
        std::vector<std::pair<std::string, std::string>> batch;
        for (const auto& [id, offsets] : recordIdToOffset) {
            std::string record = readFromWALFileById(id);
            if (!record.empty()) {
                std::cout << "+ " << id << " " << record << std::endl;
                batch.emplace_back(id, record);
            }
        }
        
        if (!batch.empty()) {
            db.flushBatchToL0(batch);
        }
        
        wal_log.truncate();
        recordIdToOffset.clear();
    }
  ```
</details>

#### 3.1 Компрессия данных

```
      - the compaction process redka-compact converts complete log chunks into SST files and merges SST files into
      bigger SST files, as every LSM database is doing. The goal is to keep the number of SST files under some limit. 
```

В качестве архитектуры `Compact` было решено использовать логику из [CockroachDB](https://www.cockroachlabs.com/docs/stable/architecture/storage-layer#lsm-levels).
Основная идея такова, что есть 10 уровней хранения SST-файлов (`L0-L9`), организованных по принципу "чем новее данные - тем ниже уровень". Данные всегда записываются в `L0`,
содержащий самые свежие записи. Каждый последующий уровень хранит более старые данные и имеет больший допустимый размер - при достижении лимита в `0^(номер уровня)` файлов
(например, 100 файлов для `L2`) запускается процесс компактизации. Во время компактизации файлы текущего уровня сливаются в один отсортированный SST-файл,
который перемещается на следующий уровень.

```C++

class LSMTree {
private:
    std::vector<std::vector<std::string>> levels;

    void ensureDbDir();
    void loadLevels();
    void mergeEntries(SSTEntry &target, const SSTEntry &source);
    void compactLevel(int level);
    std::vector<SSTEntry> readSST(const std::string &path);
    std::map<std::string, FieldValue> parseFields(const std::string &data);
    std::string serializeFields(const std::map<std::string, FieldValue> &fields);
    void writeSST(const std::string &path, const std::vector<SSTEntry> &entries);

public:
    LSMTree();
    void put(const std::string &key, const std::string &value);
    void flushBatchToL0(const std::vector<std::pair<std::string, std::string>> &batch);
    std::string get(const std::string &key);
};

```

<details>
  <summary>Код функции `compactLevel(int level)`</summary>
    
  ```C++
if (levels[level].size() >= std::pow(10, level + 1)) {
    // ... merging logic ...
    std::string new_sst = DB_DIR + "/L" + std::to_string(level + 1) + "/" + ...;
    writeSST(new_sst, entries_to_write);
    compactLevel(level + 1);  // Recursive compaction to next level
}
  ```
</details>

#### 3.2 Объединенное чтение
```
To answer a query, redka-talk merges entries from the SST files and entries from the log.
```

При запросе на чтение по ключу программа смотрит его наличие в WAL-файле и в SST-файлах. Полученные результаты мержит.
<details>
  <summary>Код функции `readRecordById(recordId)`</summary>
    
  ```C++
std::string readFromSSTFileById(const std::string& recordId) {
    std::string sstData = db.get(recordId);
    sstData = '{' + sstData + '}';
    return sstData;
}

std::string readRecordById(const std::string& recordId) {
    std::string walData = readFromWALFileById(recordId);
    
    std::string sstData = readFromSSTFileById(recordId);

    std::string merged = mergeTwoRecords(walData, sstData);

    return merged;
}
  ```
</details>

