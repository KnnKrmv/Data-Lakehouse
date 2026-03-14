# BMIS Pipeline Documentation

Bu sənəd `dags-collection` reposunun mövcud vəziyyətinə əsasən hazırlanıb və BMIS tərəfinin başdan sona necə işlədiyini izah edir. Məqsəd yalnız fayl siyahısı vermək deyil, həm də BMIS data axınının biznes və texniki məntiqini bir sənəddə toplamaqdır.

Sənədin əhatə dairəsi:

- BMIS mənbə datasının Oracle-dan Bronze qatına götürülməsi
- Silver qatında dbt transformasiyaları
- Gold qatında biznes aqreqasiyası
- Tableau üçün yekun metriklərin formalaşdırılması
- Yardımçı CSV axını və `bims_difference` snapshot məntiqi
- İstifadə olunan Airflow, Spark, Trino, MinIO, Iceberg və Nessie komponentləri
- Cari repo vəziyyətində görünən əməliyyat qeydləri və diqqət nöqtələri

Bu sənədin məqsədi yeni komanda üzvünün və ya texniki olmayan stakeholder-in BMIS pipeline-ı yüksək səviyyədə başa düşməsi, eyni zamanda mühəndisin lazım olan texniki fayla tez keçə bilməsidir.

## Vizual xəritə

Bu sənəddəki bütün diaqramlar birbaşa bu faylın içində mətn-qrafik formatında verilib. Yəni ayrıca şəkil faylı açmağa ehtiyac yoxdur.

## 1. Qısa xülasə

BMIS pipeline bir neçə ayrı mərhələdən ibarətdir:

1. Oracle mənbəsindən BMIS xam cədvəlləri Spark job-ları ilə Bronze qatına yazılır.
2. Bronze BMIS cədvəllərinin seçilmiş alt dəsti dbt ilə Silver qatında analitik ölçü və fakt modellərinə çevrilir.
3. Silver modelləri Gold qatında ya olduğu kimi publish edilir, ya da `global_transactions` kimi biznes məntiqi ilə aqreqasiya olunur.
4. Tableau üçün son istifadəçi metrikləri `bims_general_metrics` modelində formalaşdırılır.
5. Əlavə CSV mənbədən gələn fərq datası `bims_difference` cədvəli və snapshot mexanizmi ilə son hesabat qatına qoşulur.

BMIS tərəfdə ən vacib arxitektura detalı budur:

- Bronze ingest Spark ilə işləyir.
- dbt framework Spark-ı dəstəkləsə də, cari BMIS dbt model konfiqlərində əsas engine Trino-dur.
- Final Tableau modeli həm BMIS Gold datanı, həm də CSV-dən gələn müqayisə datasını birləşdirir.

## 2. Arxitektura mənzərəsi

BMIS axınının sadələşdirilmiş görünüşü:

```text
                     BMIS END-TO-END PIPELINE

  [ Oracle BMIS ]
         |
         v
  [ Bronze BMIS / Spark ingest ]
         |
         v
  [ Silver BMIS / dbt models ]
         |
         v
  [ Gold BMIS / business layer ]
         |
         v
  [ Tableau / final reporting ]

  [ CSV bims_difference ]
            |
            +------------------------------+
                                           |
                                           v
                             [ Final Tableau metrics ]
```

Bu axının repositoridəki əsas texniki nöqtələri:

- Bronze BMIS DAG: `dags/lakehouse/dags/oracle/bronze_BMIS.py`
- Silver BMIS DAG: `dags/lakehouse/dags/dbt/bmis_bronze_to_silver.py`
- Gold BMIS DAG: `dags/lakehouse/dags/dbt/bmis_silver_to_gold.py`
- Tableau DAG: `dags/lakehouse/dags/dbt/bmis_gold_tableau.py`
- CSV Bronze DAG: `dags/lakehouse/dags/maintenance/minio_csv_to_bronze.py`

### 2.1 Airflow mərhələ asılılığı

```text
start_dag_dataset
   |
   +--> 1_bronze_BMIS
   |        |
   |        v
   |   bronze_ready/bmis
   |        |
   |        v
   |   2_silver_BMIS
   |        |
   |        v
   |   silver_ready/bmis
   |        |
   |        v
   |   3_gold_BMIS
   |        |
   |        v
   |   gold_ready/bmis
   |        |
   |        v
   |     Tableau
   |        |
   |        v
   |   tableau_ready/bmis
   |
   +--> csv-files
            |
            v
     bronze.bmis.bims_difference
```

### 2.2 Əsas data lineage

```text
Bronze sources
--------------
bcm_document_cost_amounts ----+
bcm_documents ----------------+--> s_financial_transactions --> gold.financial_transactions --+
dict_exec_budget_info --------+                                                                  |
                                                                                                 |
functional/economic/admin dictionaries --> silver dimensions --> gold dimensions ----------------+--> global_transactions --> bims_general_metrics --> Tableau
plant + LOV sources ---------------------> silver dimensions --> gold dimensions ----------------+

CSV bims_difference --> bronze.bmis.bims_difference --> silver.bmis.bims_difference -----------+
```

## 3. Texniki stack və platform komponentləri

BMIS pipeline aşağıdakı əsas platform komponentləri üzərində qurulub:

### 3.1 Airflow

Airflow burada yalnız scheduler deyil, həm də pipeline orkestratorudur:

- DAG-ların işə düşmə ardıcıllığını idarə edir
- Connection və Variable-ları inject edir
- Dataset-lər vasitəsilə mərhələlər arasında asılılıq qurur
- Spark və dbt task-larını Kubernetes pod-ları kimi işlədərək izolə edir

BMIS axını cron əsaslı deyil, dataset-driven yanaşmadan istifadə edir:

- `1_bronze_BMIS` DAG-ı `lakehouse/start_dag_dataset` dataset-i ilə start alır
- `2_silver_BMIS` DAG-ı `lakehouse/bronze_ready/bmis` dataset-inə bağlıdır
- `3_gold_BMIS` DAG-ı `lakehouse/silver_ready/bmis` dataset-inə bağlıdır
- `Tableau` DAG-ı `lakehouse/gold_ready/bmis` dataset-inə bağlıdır

Bu quruluşun üstünlüyü odur ki, hər mərhələ əvvəlki qat həqiqətən hazır olanda işə düşür.

### 3.2 Spark

Spark BMIS tərəfində əsasən Bronze ingest üçün istifadə olunur:

- Oracle JDBC oxunuşu
- Iceberg cədvəlinə yazılış
- CSV-dən Bronze cədvələ ingest
- ayrı maintenance və housekeeping task-ları

Spark session `create_spark()` helper-i ilə qurulur və aşağıdakı komponentləri özünə bağlayır:

- MinIO üzərindən S3A storage
- Nessie catalog
- Iceberg table format
- Bronze/Silver/Gold üçün ayrıca warehouse path-ləri

Default konfiqurasiyada:

- MinIO endpoint: `http://10.254.8.26:9000`
- bucket: `datalakehouse`
- Bronze warehouse: `s3a://datalakehouse/bronze`
- Silver warehouse: `s3a://datalakehouse/silver`
- Gold warehouse: `s3a://datalakehouse/gold`
- Nessie URI: `http://10.254.8.105:19120/api/v2`

### 3.3 Trino və dbt

dbt framework həm Trino, həm Spark engine ilə işləyə bilir. Amma cari BMIS vəziyyətində:

- Silver model YAML-larında engine açıq şəkildə `trino` seçilib
- Gold və Tableau YAML-larında engine göstərilmədiyi üçün default olaraq `trino` işləyir
- `spark` engine üçün framework və connection helper-lər mövcuddur, amma BMIS current flow-da aktiv seçim deyil

Bu o deməkdir ki, Spark daha çox ingest qatında, Trino isə analytics transform qatında istifadə olunur.

### 3.4 MinIO + Iceberg + Nessie

Storage və table layer kombinasiyası belədir:

- Fiziki fayllar MinIO-da saxlanılır
- Table format Iceberg-dir
- Catalog və branching Nessie ilə idarə olunur

Layer-lər ayrı logical branch kimi işləyir:

- `bronze`
- `silver`
- `gold`

Bu yanaşma həm versiyalama, həm də qatların bir-birindən ayrılması baxımından faydalıdır.

## 4. BMIS pipeline-ın sonadək ümumi axını

BMIS project-in texniki axını aşağıdakı kimidir:

### 4.1 Bronze

Oracle BMIS cədvəlləri Spark ingestion job-u ilə `bronze.bmis.*` cədvəllərinə yüklənir.

### 4.2 Silver

Bronze qatdakı seçilmiş cədvəllərdən istifadə olunaraq analitik ölçü və fakt modelləri hazırlanır:

- inzibati təsnifat
- funksional təsnifat
- iqtisadi təsnifat
- büdcə proqramı
- plant strukturu
- lookup/lov kateqoriyaları
- maliyyə tranzaksiyaları

### 4.3 Gold

Silver modelləri gold qatına publish edilir və əsas biznes summary cədvəli `global_transactions` yaradılır.

### 4.4 Tableau

Final layer-də:

- BMIS gold metrikləri
- CSV-dən gələn müqayisə datası

bir modeldə birləşdirilir və əvvəlki illə müqayisə göstəriciləri hesablanır.

## 5. Bronze qat: Oracle-dan BMIS xam datasının götürülməsi

BMIS Bronze ingestion-un əsas orkestrasiya faylı:

- `dags/lakehouse/dags/oracle/bronze_BMIS.py`

Bu DAG aşağıdakı işi görür:

- `yaml_files/bmis` qovluğundakı bütün `.yml` faylları oxuyur
- hər YAML üçün ayrıca Spark task yaradır
- Oracle connection məlumatlarını environment variable kimi pod-a ötürür
- Spark container daxilində `jobs/spark/ingest_table.py` skriptini işlədir
- bütün task-lar uğurla bitəndən sonra `lakehouse_vars` daxilində `bronze_bmis` pəncərəsinin `end_date` dəyərini bir gün artırır
- sonda `lakehouse/bronze_ready/bmis` dataset-ini çıxış kimi emit edir

### 5.1 Bronze BMIS həcmi

Cari repo vəziyyətində BMIS Bronze qovluğunda:

- 245 ədəd YAML ingest config var
- 242 cədvəl `truncate_insert` rejimində yüklənir
- 3 cədvəl `incremental_insert` rejimində yüklənir
- ayrıca `partitionColumn`, `numPartitions`, `write_repartition` konfiqi görünmür

Vizual olaraq bu bölgü belə görünür:

```text
BMIS Bronze Load Mode Distribution

truncate_insert     242  |##################################################| 98.8%
incremental_insert    3  |#                                                 |  1.2%

Total configs: 245
```

Bu rəqəm onu göstərir ki, BMIS Bronze layer çox geniş xam operational sahəni əhatə edir. Amma bunun hamısı hazırkı analytics flow-da istifadə olunmur.

### 5.2 Bronze BMIS task quruluşu

Bronze DAG hər YAML faylı üçün təxminən bu məntiqi tətbiq edir:

1. Oracle connection məlumatlarını Airflow connection-dan götürür.
2. MinIO connection məlumatlarını inject edir.
3. `PY_FILE=/opt/lakehouse/jobs/spark/ingest_table.py` ötürür.
4. `APP_ARGS=--config_path ...` ilə hansı YAML-ın işlənəcəyini bildirir.
5. `WINDOW_KEY=bronze_bmis` və `LAKEHOUSE_VARS` ötürür.
6. Task-ı `KubernetesMOFSparkOperator` ilə `spark-jobs` namespace-də işə salır.

BMIS Bronze üçün Spark resurs profili `large` seçilib:

- driver cores: 4
- driver memory: 4g
- executor cores: 4
- executor memory: 8g
- executor count: 6

### 5.3 Spark ingestion mexanizmi

`jobs/spark/ingest_table.py` və `jobs/spark/ingest_helpers.py` birlikdə aşağıdakı işi görür:

- YAML config oxunur
- source connection və query file müəyyən edilir
- SQL faylı yüklənir
- lazım olsa incremental date filter əlavə olunur
- Spark JDBC reader ilə source data oxunur
- lazım olsa read audit edilir
- lazım olsa repartition edilir
- Iceberg cədvəlinə yazılır

Yükləmə rejimləri:

- `truncate_insert`
- `append`
- `incremental_insert`
- `merge`

BMIS Bronze tərəfdə əsasən ilk ikisi deyil, `truncate_insert` və bəzi yerlərdə `incremental_insert` istifadə olunur.

### 5.4 BMIS-də incremental işləyən Bronze cədvəllər

Cari repo-da `incremental_insert` işləyən 3 BMIS cədvəli bunlardır:

1. `bronze.bmis.bims_data_bcm_documents`
   - incremental date column: `current_year`
2. `bronze.bmis.bims_data_bcm_document_cost_amounts`
   - incremental date column: `h_date`
3. `bronze.bmis.bims_dict_exec_budget_info`
   - incremental date column: `current_year`

Bu cədvəllər BMIS analytics flow-un mərkəzindədir, çünki sonrakı `financial_transactions` modeli məhz bunların üzərində qurulur.

### 5.5 Bronze config formatı

BMIS YAML-ları çox sadə və config-driven yanaşma ilə yazılıb. Tipik nümunə:

```yaml
source:
  connection: ORACLE
  query_file: "../../sql_query/bmis/some_table.sql"
target:
  table: "bronze.bmis.some_table"
load:
  mode: "truncate_insert"
```

Incremental nümunədə əlavə olaraq:

```yaml
load:
  mode: "incremental_insert"
  incremental:
    date_column: "current_year"
```

Bu yanaşmanın üstünlüyü ondadır ki, yeni BMIS source əlavə etmək üçün çox vaxt Python kodunu dəyişmək lazım gəlmir; yeni SQL + YAML kifayət edir.

### 5.6 SQL source qovluğu

Hər BMIS ingest config öz SQL faylına işarə edir:

- YAML qovluğu: `dags/lakehouse/dags/oracle/yaml_files/bmis`
- SQL qovluğu: `dags/lakehouse/dags/oracle/sql_query/bmis`

Bu ayrım komandaya aşağıdakı üstünlükləri verir:

- extraction məntiqi SQL fayllarda saxlanılır
- orchestration məntiqi isə YAML və DAG səviyyəsində qalır
- dəyişikliklər daha izolyasiyalı olur

### 5.7 Bronze ingestion sequence

Bronze ingestion ardıcıllığı sadə şəkildə belə işləyir:

1. Airflow hər YAML üçün ayrıca Spark task yaradır.
2. Spark job `ingest_table.py` vasitəsilə uyğun SQL faylı və config-i oxuyur.
3. JDBC üzərindən Oracle source datanı qaytarır.
4. Job load mode və incremental qaydaları tətbiq edir.
5. Nəticə Iceberg Bronze cədvəlinə yazılır.
6. DAG bütün task-lar bitəndən sonra növbəti mərhələ üçün dataset çıxarır.

## 6. `bims_difference`: CSV-dən gələn əlavə mənbə

BMIS final layer yalnız Oracle datasından qidalanmır. `bims_difference` adlı ayrıca bir mənbə də mövcuddur.

Bu axının texniki mənbəyi:

- DAG: `dags/lakehouse/dags/maintenance/minio_csv_to_bronze.py`
- Spark job: `dags/lakehouse/jobs/spark/ingest_csv_to_bronze.py`
- hədəf cədvəl: `bronze.bmis.bims_difference`

### 6.1 CSV axının işi

Bu DAG hər 1 dəqiqədən bir işləyir və MinIO-da aşağıdakı faylı gözləyir:

- bucket: `csv-files`
- key pattern: `bmis_metrics/*.csv`
- əsas fayl adı: `bmis_umumi_gostericiler.csv`

CSV tapıldıqda:

1. Spark job CSV-ni oxuyur
2. Azərbaycan dilində kolon adlarını daxili analitik kolon adlarına map edir
3. metadata kolonları əlavə edir:
   - `ingest_timestamp`
   - `ingest_date`
   - `source_file`
   - `source_path`
4. Iceberg cədvələ yazır
5. CSV faylını bucket-dən silir

### 6.2 `bims_difference` niyə vacibdir

Final Tableau model `bims_general_metrics` iki fərqli mənbədən data alır:

- `gold.bmis.global_transactions` üzərindən BMIS hesablanmış göstəricilər
- `silver.bmis.bims_difference` üzərindən CSV mənbəli fərq datası

Bu səbəbdən `bims_difference` son mərhələdə biznes baxımından köməkçi yox, faktiki olaraq yekun hesabatın bir hissəsidir.

### 6.3 Snapshot qeydi

Repo-da `dbt/snapshots/bmis/bims_difference.sql` snapshot faylı mövcuddur. O, Bronze-dakı `bims_difference` cədvəlindən Silver qatında tarixçələnən snapshot saxlamalıdır.

Amma cari repo vəziyyətində `bmis_silver.yml` içində snapshot task-lar şərhə salınıb, yəni aktiv model listinə daxil deyil. Bu o deməkdir ki:

- snapshot məntiqi kod bazasında mövcuddur
- amma current BMIS Bronze-to-Silver DAG konfiqində aktiv görünmür

Praktik işlək mühitdə bu snapshot ayrıca job ilə və ya manual dbt snapshot run ilə yenilənə bilər. Sənəddə bunu açıq qeyd etmək vacibdir, çünki final Tableau qatındakı CSV hissəsinin yenilənməsi bu nöqtədən asılıdır.

## 7. Silver qat: BMIS analitik modellərinin qurulması

Silver mərhələsinin əsas DAG faylı:

- `dags/lakehouse/dags/dbt/bmis_bronze_to_silver.py`

Bu DAG:

- `lakehouse/bronze_ready/bmis` dataset-i gələndə işə düşür
- `yaml_files/bronze_to_silver/bmis/bmis_silver.yml` faylını oxuyur
- həmin YAML-dakı model siyahısına görə dbt task-lar yaradır
- task dependency-lərini YAML əsasında qurur
- sonda `lakehouse/silver_ready/bmis` dataset-i çıxarır

### 7.1 Silver qatın cari modeli

Cari konfiqurasiyada 7 aktiv BMIS Silver modeli var:

1. `s_administrative_class`
2. `s_budget_program`
3. `s_economics_class`
4. `s_functional_class`
5. `s_lov_category`
6. `s_plant`
7. `s_financial_transactions`

Bu modellərin hamısı current YAML-da `engine: trino` ilə göstərilib.

Silver qatın model əlaqəsi sadələşdirilmiş formada:

| Bronze-dan yaranan Silver model | Rol |
| --- | --- |
| `administrative_class` | inzibati təsnifat ölçüsü |
| `budget_program` | büdcə proqramı ölçüsü |
| `economics_class` | iqtisadi təsnifat ölçüsü |
| `functional_class` | funksional təsnifat ölçüsü |
| `lov_category` | lookup və category dəyərləri |
| `plant` | təşkilati/struktur ierarxiyası |
| `financial_transactions` | əsas fakt cədvəli |

### 7.2 Silver üçün istifadə olunan Bronze mənbələr

`src_bronze_bmis.yml` daxilində hazırkı analytics flow üçün 23 source cədvəl təyin olunub. Bu o deməkdir ki, Bronze BMIS-də 245 cədvəl olsa da, Silver current analytics yalnız seçilmiş alt dəstdən istifadə edir.

Əsas istifadə olunan mənbələr bunlardır:

- əməliyyat/fakt mənbələri:
  - `bims_data_bcm_document_cost_amounts`
  - `bims_data_bcm_documents`
  - `bims_dict_exec_budget_info`
  - `bims_data_plant`
- təsnifat və lookup mənbələri:
  - `bims_dict_economic_classification`
  - `bims_dict_functional_classification`
  - `bims_dict_admin_class_budget_main_orgs`
  - `bims_dict_admin_class_groups`
  - `bims_dict_admin_class_sources`
  - `bims_dict_regions`
  - `bims_dict_document_statuses`
  - `bims_dict_budgetary_report_terms`
  - `bims_dict_budgetary_types`
  - `bims_dict_budget_types`
  - `bims_dict_budget_program`
  - `bims_dict_treasury_agencies`
  - `bims_dict_plant_types`
  - `bims_dict_plant_levels`
  - `bims_dict_branch_departments`
- əlavə mənbə:
  - `bims_difference`

### 7.3 Silver model: `administrative_class`

Bu model inzibati təsnifatı üçsəviyyəli matrisa kimi formalaşdırır:

- source
- group
- organization

Model:

- `bims_dict_admin_class_sources`
- `bims_dict_admin_class_groups`
- `bims_dict_admin_class_budget_main_orgs`

cədvəllərini birləşdirir və hər sətir üçün:

- `final_organization_code`
- `matrix_level`
- `final_section_name`
- `flag`
- `global_code`

yaradır.

Buradakı `flag` aktiv qeydləri göstərmək üçün istifadə olunur:

- `year(end_date) = 2100` və `deleted = 0` olduqda aktiv sayılır

Bu pattern BMIS dimension modellərində tez-tez təkrar olunur.

### 7.4 Silver model: `functional_class`

`s_functional_class.sql` funksional təsnifatı hierarchical struktura çevirir:

- section
- subsection
- paragraph

Bu modelin əsas rolu:

- BMIS funksional kodlarını analitik friendly ölçü cədvəlinə çevirmək
- final reporting üçün həm kod, həm də ad səviyyəsini saxlamaq

`final_section_id` və `final_section` kolonları sonrakı join-lərdə əsas rol oynayır.

### 7.5 Silver model: `economics_class`

`s_economics_class.sql` iqtisadi təsnifatı daha dərin hierarxiya ilə qurur:

- section
- subsection
- paragraph
- article
- sub_article

Bu modeldə həm aktiv, həm də qeyri-aktiv təsnifatlar üçün `flag` məntiqi saxlanılır. Bunun məqsədi tarixi və ya silinmiş kodların izlənməsini tam itirməməkdir.

### 7.6 Silver model: `plant`

`s_plant.sql` BMIS təşkilati/struktur ağacını bir neçə səviyyədə açır:

- section
- subsection
- category
- subcategory
- org

Model eyni zamanda:

- test məqsədli adları filtrdən çıxarır
- final səviyyə identifikatorunu hesablayır
- aktivlik flag-i yaradır

Bu cədvəl sonradan `global_transactions` içində `plant_code = 2` kimi biznes filtrlərdə dolayı olaraq əhəmiyyət kəsb edir.

### 7.7 Silver model: `lov_category`

`s_lov_category.sql` müxtəlif lookup cədvəllərini vahid modeldə birləşdirir. Burada fərqli source-lardan gələn lookup dəyərlər aşağıdakı kimi standard bir formata salınır:

- `fid`
- `value_name`
- `value_desc`
- `start_date`
- `end_date`
- `flag`

Bu model reporting qatında çox vacibdir, çünki:

- region adları
- budget type
- budgetary type
- budget term
- branch departments
- plant type
- plant level
- treasury agency
- document status

kimi human-readable dəyərləri təmin edir.

### 7.8 Silver model: `budget_program`

Bu model `bims_dict_budget_program` cədvəlini analitik istifadə üçün sadələşdirilmiş formatda təqdim edir:

- program id
- year
- budget program name
- value date
- active flag

### 7.9 Silver model: `financial_transactions`

Silver qatın ən kritik modeli `s_financial_transactions.sql`-dır. Bu model BMIS-in fakt təbəqəsinin əsasını təşkil edir.

Model iki əsas mənbəni birləşdirir:

1. `bims_data_bcm_document_cost_amounts` + `bims_data_bcm_documents`
2. `bims_dict_exec_budget_info`

Bu modelin əsas biznes işi:

- wide format-da olan məbləğ kolonlarını `UNNEST` vasitəsilə long format-a çevirmək
- hər amount-u ayrıca `amount_type` kimi saxlamaq
- dimension join-lərə uyğun kodları çıxarmaq
- transaction status yaratmaq (`CONFIRM` / `CANCEL`)
- hesabat tarixi və `global_code` əlavə etmək

Bu modeldə yaranan əsas `amount_type` nümunələri:

- `OLD_YEAR_REVIEW_AMOUNT`
- `CURRENT_YEAR_BUDGET_AMOUNT`
- `CURRENT_YEAR_EXPECTED_AMOUNT`
- `NEXT_YEAR_ORDER_AMOUNT`
- `NEXT_YEAR_ACCEPTED_AMOUNT`
- `EXECUTE`
- `BUDGET`
- `CURRENT_EXPECTED`
- `EXECUTE_6_MONTH`

Başqa sözlə, `financial_transactions` modeli BMIS-in müxtəlif məbləğ sütunlarını bir standart fakt strukturuna çevirir.

### 7.10 Silver qatın incremental qeydi

Silver modellərin bir çoxu incremental materialization istifadə edir, amma cari BMIS dbt SQL-lərində `dbt var()` istifadəsi görünmür. Yəni DAG səviyyəsində `start_date` və `end_date` ötürülsə də, checked BMIS SQL modelləri bu dbt var-ları birbaşa istifadə etmir.

Əvəzində bəzi modellərdə incremental guard hard-coded şəkildə yazılıb, məsələn:

- `where current_year >= 2026`
- `where value_date >= 2026-01-01`
- `where year(ft.value_date) >= 2026`

Bu sənəd üçün vacib qeyddir, çünki pipeline pəncərə idarəetməsi ilə model daxilindəki faktiki filtr həmişə eyni anlayış olmaya bilər.

## 8. Gold qat: BMIS biznes qatının formalaşdırılması

Gold mərhələsinin əsas DAG faylı:

- `dags/lakehouse/dags/dbt/bmis_silver_to_gold.py`

Bu DAG:

- `lakehouse/silver_ready/bmis` dataset-ini gözləyir
- `yaml_files/silver_to_gold/bmis/bmis_gold.yml` faylındakı modelləri işə salır
- sonda `lakehouse/gold_ready/bmis` dataset-i çıxarır

### 8.1 Gold qatın aktiv modelləri

Cari konfiqurasiyada 8 model var:

1. `financial_transactions`
2. `functional_class`
3. `economics_class`
4. `administrative_class`
5. `lov_category`
6. `plant`
7. `budget_program`
8. `global_transactions`

Bu modellərdən ilk 7-si faktiki olaraq Silver-dən Gold-a publish rolu oynayır. Yəni əsas biznes hesablanması Gold-da `global_transactions` modelində baş verir.

Gold qatın dependency görünüşü:

`global_transactions` modeli aşağıdakı Gold obyektlərindən qidalanır:

- `financial_transactions`
- `functional_class`
- `economics_class`
- `administrative_class`
- `lov_category`
- `plant`
- `budget_program`

### 8.2 Gold publish modelləri

Aşağıdakı modellər Silver source-dan `select *` və ya ona yaxın sadə publish yanaşması ilə işləyir:

- `financial_transactions`
- `functional_class`
- `economics_class`
- `administrative_class`
- `plant`
- `budget_program`
- `lov_category`

Bu yanaşmanın məqsədi:

- Tableau və downstream layer-lər üçün Gold schema-da stabil obyekt saxlamaq
- analitik join-ləri bir qatda toplamaq
- sonrakı hesabatların Silver-ə birbaşa bağlılığını azaltmaq

### 8.3 Gold model: `global_transactions`

`global_transactions.sql` BMIS Gold qatın ən vacib modelidir. Bu model:

- `financial_transactions` faktını
- funksional təsnifatı
- iqtisadi təsnifatı
- inzibati təsnifatı
- büdcə proqramını
- lookup/lov kateqoriyalarını
- plant strukturunu

bir araya gətirir və son hesabat üçün konsolidə məbləğləri hesablayır.

#### 8.3.1 Əsas ölçülər

Model nəticəsində aşağıdakı əsas ölçü sahələri yaranır:

- functional code / section / subsection / paragraph
- economic code / section / subsection / paragraph / article / subarticle
- budget program code / name
- administrative code
- administrative source / group / organization
- budget name
- budgetary name
- budget term name
- region name
- branch name
- current status name
- year
- report date

#### 8.3.2 Əsas ölçülən metriklər

Model bu məbləğləri hesablayır:

- `actual_execution_amount`
- `confirmed_budget_amount`
- `expected_execution_amount`
- `order_amount`
- `accepted_amount`

#### 8.3.3 Əsas biznes filtrləri

Cari SQL məntiqində aşağıdakı filtr qaydaları görünür:

- yalnız `trn_status = 'CONFIRM'`
- `region_code <> 2`
- `budget_code in (1, 2)`
- `plant_code = 2`
- `functional_code <> 572`
- `year(ft.value_date) between 2022 and 2026`
- `ft.amount != 0`
- `budget_term_code in (3, 21)`

Bu filtrlər çox vacibdir, çünki final metriklərin biznes mənasını birbaşa dəyişir.

#### 8.3.4 İncremental davranış

Model incremental append strategiyası ilə işləyir və `pre_hook` vasitəsilə:

- `year >= 2026` olan mövcud sətirləri əvvəlcə silir

Sonra həmin period üçün yenidən hesablayır. Yəni bu model tam append kimi görünsə də, cari period üçün refresh davranışı var.

### 8.4 `part` dəyişəni haqqında qeyd

Gold BMIS DAG `dbt_vars` içində `part` adlı dəyişən də ötürür:

- `var_financial_transaction`

Lakin checked BMIS model SQL-lərində `var()` istifadəsi görünmür. Yəni bu dəyişən hazırkı BMIS model logic-də aktiv istifadə olunmur və gələcək genişlənmə və ya köhnə dizayn qalığı ola bilər.

## 9. Tableau qat: son istifadəçi üçün BMIS metrikləri

Final layer-in əsas DAG faylı:

- `dags/lakehouse/dags/dbt/bmis_gold_tableau.py`

Bu DAG:

- `lakehouse/gold_ready/bmis` dataset-ini gözləyir
- `yaml_files/gold_to_tableau/bmis/bmis_tableau.yml` faylını oxuyur
- sonda `lakehouse/tableau_ready/bmis` dataset-i çıxarır

### 9.1 Tableau qatın modelləri

Cari konfiqurasiyada 2 model var:

1. `bims_general_metrics`
2. `bims_general_metrics_old`

Praktik olaraq əsas işlək model `bims_general_metrics` hesab oluna bilər, `old` versiyası isə legacy və ya müqayisə məqsədli görünür.

Final Tableau modelinin məntiqi:

```text
gold.bmis.global_transactions -----------+
                                         |
                                         v
                               bims_general_metrics
                                         |
silver.bmis.bims_difference ------------+
                                         |
                                         v
                                Tableau dashboards
```

### 9.2 `bims_general_metrics` modelinin rolu

Bu model final report cədvəlidir. O:

- BMIS-dən gələn hesablanmış göstəriciləri
- CSV-dən gələn `bims_difference` snapshot datasını

vahid struktura salır.

Modelin hədəf yeri:

- database: `gold`
- schema: `tableau`
- alias: `bims_general_metrics`

Yəni final output `gold.tableau.bims_general_metrics` kimi düşünülə bilər.

### 9.3 Modelin iki əsas mənbəsi

Model iki hissədən ibarətdir:

#### A. BMIS hissəsi

`gold.bmis.global_transactions` əsasında:

- actual execution
- budget
- expected execution
- order
- accepted

məbləğləri funksional, iqtisadi, təşkilati və proqram kəsimində toplayır.

#### B. CSV hissəsi

`silver.bmis.bims_difference` əsasında:

- xarici mənbədən gələn fərq və ya əl ilə hazırlanmış göstəriciləri
- `source_name = 'CSV'`

olaraq əlavə edir.

### 9.4 Əvvəlki il müqayisəsi

`bims_general_metrics` modelində `LAG()` window funksiyaları ilə aşağıdakı əvvəlki il sahələri hesablanır:

- `prev_actual_execution_amount`
- `prev_budget_amount`
- `prev_expected_execution_amount`
- `prev_order_amount`
- `prev_accepted_amount`

Bu, Tableau tərəfində growth, müqayisə və trend analizi üçün vacibdir.

### 9.5 `source_name` və `sign` sahələri

Final modeldə mənbələr fərqləndirilir:

- BMIS-dən gələn sətirlər üçün:
  - `source_name = 'BMIS'`
  - `sign = 'Y'`
- CSV-dən gələn sətirlər üçün:
  - `source_name = 'CSV'`
  - `sign = 'N'`

Bu sahələr downstream BI səviyyəsində data provenance və vizual ayrım üçün faydalıdır.

Mənbə paylanmasının məntiqi qısa formada belədir:

- `source_name = 'BMIS'` olan sətirlər əsas BMIS hesablanmış metrikləridir.
- `source_name = 'CSV'` olan sətirlər əlavə müqayisə və ya köməkçi datadır.
- final dataset həm cari dövr göstəricilərini, həm də əvvəlki il müqayisə sahələrini saxlayır.

### 9.6 Tableau qatın incremental qaydası

Model `pre_hook` ilə aşağıdakı cleanup edir:

- `year >= 2026 AND source_name = 'BMIS'` olan sətirləri silir
- `source_name = 'CSV'` olan sətirləri silir

Sonra təzə nəticəni append edir. Bu davranış onu göstərir ki:

- BMIS hissəsi cari illər üçün təzələnir
- CSV hissəsi isə hər refresh-də tam yenidən yığılır

## 10. Airflow dəyişənləri və pəncərə idarəetməsi

BMIS pipeline bir neçə fərqli variable və window mexanizmindən istifadə edir.

### 10.1 Bronze tərəf

Bronze BMIS DAG aşağıdakı açarları istifadə edir:

- `lakehouse_vars`
- `WINDOW_KEY=bronze_bmis`

`lakehouse_vars` JSON obyektidir və içində hər flow üçün `start_date` / `end_date` saxlanır. Bronze DAG iş bitəndə `bronze_bmis.end_date` dəyərini 1 gün artırır, amma bugünkü tarixi keçmir.

### 10.2 Silver tərəf

Silver BMIS DAG dbt-yə bu dəyişənləri ötürür:

- `var_silver_bmis.start_date`
- `var_silver_bmis.end_date`

və bitəndə `var_silver_bmis` variable-ının `end_date` hissəsini 1 gün artırır.

### 10.3 Gold tərəf

Gold BMIS DAG:

- `var_gold_bmis.start_date`
- `var_gold_bmis.end_date`
- `var_financial_transaction`

ötürür və sonda `var_gold_bmis.end_date` dəyərini artırır.

### 10.4 Tableau tərəf

Tableau DAG:

- `var_gold_tableau.start_date`
- `var_gold_tableau.end_date`

ötürür və sonda `var_gold_tableau.end_date` dəyərini artırır.

### 10.5 Praktik qeyd

Cari checked BMIS SQL modellərində `dbt var()` istifadəsi görünmədiyi üçün bu dəyişənlər əsasən orchestration səviyyəsində saxlanılır. Bunun nəticəsi olaraq:

- Airflow variable pəncərələri mövcuddur
- lakin SQL səviyyəsində real incremental məntiq bir çox halda hard-coded date condition-larla idarə olunur

Bu gələcək refaktor üçün də diqqət nöqtəsidir.

## 11. BMIS tərəfində Spark istifadəsi və istifadəsiz qalan imkanlar

BMIS tərəfində Spark üçün iki fərqli vəziyyət var:

### 11.1 Aktiv Spark istifadəsi

Hal-hazırda aktiv istifadə olunan Spark hissələri:

- Oracle -> Bronze BMIS ingest
- CSV -> Bronze `bims_difference` ingest
- ümumi Spark submit və resource profilləri

### 11.2 Framework-də mövcud, amma BMIS-də hazırda aktiv olmayan Spark imkanları

dbt tərəfində:

- `base_dbt_dag.py` Spark Thrift connection-u dəstəkləyir
- `spark_dbt/profiles.yml` mövcuddur
- `bmis_silver.yml` içində Spark model nümunələri comment şəklində saxlanılıb

Amma current BMIS model konfiqlərində aktiv `engine: spark` seçimi görünmür.

Bu o deməkdir ki, komanda istəsə gələcəkdə böyük həcmli dbt transformları Spark-a köçürə bilər, amma bu, hazırkı BMIS repo davranışı deyil.

## 12. Cari repo vəziyyətinə əsasən əsas diqqət nöqtələri

Bu bölmə dokumentasiya üçün vacibdir, çünki “sistem idealda necə işləməlidir” ilə “repo-da hazırda necə görünür” bəzən fərqlənir.

### 12.1 Bronze genişdir, analytics alt dəsti dardır

BMIS Bronze qatı çox genişdir: 245 config. Amma Silver current analytics yalnız 23 source cədvəldən istifadə edir.

Bu o deməkdir ki:

- Bronze daha çox arxiv/xam data platforması rolundadır
- current BMIS BI axını isə bu datanın yalnız müəyyən biznes hissəsini istifadə edir

### 12.2 BMIS Bronze-də partition config görünmür

Cari BMIS YAML-larında:

- `partitionColumn`
- `numPartitions`
- `write_repartition`

görünmür.

Bu, ingest job-ların Spark JDBC parallel read imkanı olsa da, BMIS config-lərində onun aktiv edilmədiyini göstərir.

### 12.3 dbt var-ları ötürülür, amma SQL-də görünmür

Airflow dbt task-larına `start_date`, `end_date`, `part` ötürür. Amma checked BMIS SQL-lərində `var()` istifadəsi görünmür.

Bu fərq:

- sənədləşmədə mütləq qeyd olunmalıdır
- çünki sırf DAG-a baxan biri bu vars-ların aktiv filter olduğunu düşünə bilər

### 12.4 `bims_difference` üçün iki mərhələli asılılıq var

Final Tableau model CSV datasından asılıdır, lakin bunun üçün:

1. CSV Bronze ingest işləməlidir
2. `bims_difference` snapshot Silver-də mövcud olmalıdır

Repo-da snapshot faylı var, amma current BMIS silver model YAML-da snapshot task-ları comment vəziyyətindədir. Bu səbəbdən əməliyyat komandasının həmin hissənin faktiki necə refresh olunduğunu ayrıca bilməsi vacibdir.

### 12.5 Gold qatın əsas biznes məntiqi bir modeldə cəmlənib

Gold-dakı əsas transformasiya `global_transactions` modelindədir. Qalan Gold modellər daha çox publish və semantic placement rolunu daşıyır.

Bu o deməkdir ki:

- BMIS nəticələrində uyğunsuzluq varsa, ilk baxılacaq model `global_transactions` olmalıdır
- Tableau nəticəsində uyğunsuzluq varsa, ikinci baxılacaq model `bims_general_metrics` olmalıdır

## 13. Əməliyyat baxımından troubleshooting xəritəsi

Bu bölmə praktik istifadə üçün faydalıdır.

### 13.1 Bronze problem olduqda

Yoxlanmalı yerlər:

- `bronze_BMIS.py`
- uyğun YAML config
- uyğun SQL query file
- Airflow Oracle connection (`oracle_lakehouse`)
- Spark pod log-ları
- MinIO credentials və bucket path

Tipik səbəblər:

- Oracle query error
- env variable çatışmazlığı
- Iceberg target schema problemi
- timeout və ya resource problemi

### 13.2 Silver problem olduqda

Yoxlanmalı yerlər:

- `bmis_bronze_to_silver.py`
- `bmis_silver.yml`
- model SQL faylı
- upstream Bronze table-ların hazır olması
- Trino connection və dbt logs

Tipik səbəblər:

- source table gecikməsi
- join key mismatch
- deleted/end_date logic nəticəsində boş nəticə
- SQL sintaksis problemi

### 13.3 Gold problem olduqda

Əsas baxış nöqtələri:

- `global_transactions.sql`
- `financial_transactions` modelinin doluluğu
- `lov_category` mapping-ləri
- filter-lərdəki kod və year qaydaları

Tipik uyğunsuzluqlar:

- məbləğlər sıfır gəlir
- region və ya budget filtri gözlənilməyən nəticə verir
- lookups name sahələrini boş qaytarır

### 13.4 Tableau problem olduqda

Əsas baxış nöqtələri:

- `bims_general_metrics.sql`
- `gold.bmis.global_transactions`
- `silver.bmis.bims_difference`
- CSV Bronze DAG və snapshot yenilənməsi

Tipik uyğunsuzluqlar:

- CSV hissə görünmür
- əvvəlki il göstəriciləri boş gəlir
- source_name ayrımı qarışır
- BMIS və CSV sətirləri gözlənilən kimi refresh olunmur

## 14. BMIS pipeline-ın mərhələ-mərhələ biznes izahı

Bu bölmə texniki olmayan auditoriya üçün sadə dildə izahdır.

### 14.1 Mənbə sistemi

BMIS sistemi müxtəlif büdcə, təsnifat, təşkilat, plan və fakt məlumatlarını Oracle-da saxlayır.

### 14.2 Bronze nə edir

Bronze qat bu məlumatı mümkün qədər xam şəkildə lakehouse-a gətirir. Məqsəd mənbə sistemdən ayrılmış, reproducible bir xam qat yaratmaqdır.

### 14.3 Silver nə edir

Silver qat xam BMIS sahələrini analitik dildə istifadə oluna bilən modellərə çevirir:

- kodlar daha anlaşılan struktura salınır
- ierarxiyalar açılır
- müxtəlif məbləğ sütunları standart tranzaksiya quruluşuna gətirilir

### 14.4 Gold nə edir

Gold qat hesabat və dashboard baxımından istifadə oluna bilən biznes göstəricilərini hazırlayır. Əsas məqsəd artıq raw data saxlamaq deyil, qərarvermə üçün hazır ölçü və metriklər formalaşdırmaqdır.

### 14.5 Tableau nə edir

Final qat BMIS və əlavə CSV mənbəli göstəriciləri birləşdirib vizuallaşdırma və müqayisə üçün uyğun cədvəl yaradır.

## 15. Nəticə

BMIS project bu repositoridə sadə bir ETL deyil, tam çoxqatlı lakehouse pipeline kimi qurulub:

- Spark ilə ingestion
- Iceberg/Nessie ilə layer-lənmiş storage
- dbt ilə semantic transform
- Tableau üçün ayrıca final publish qat

Hazırkı repo vəziyyətinə görə BMIS layihəsinin ən vacib xüsusiyyətləri bunlardır:

- Oracle-dan 245 BMIS source config Bronze qatına yüklənir
- analytics flow həmin xam qatın seçilmiş alt dəstindən istifadə edir
- Silver-də 7 əsas analitik model qurulur
- Gold-da 8 modeldən ibarət biznes qat var və əsas hesablanma `global_transactions` modelində toplanır
- Tableau üçün `bims_general_metrics` BMIS və CSV datasını birləşdirir
- framework Spark-ı dəstəkləsə də, cari BMIS dbt transformları Trino ilə işləyir
- `bims_difference` axını son hesabat üçün vacibdir və ayrıca diqqət tələb edir

Əgər bu sənədin növbəti versiyası hazırlanacaqsa, onu daha da gücləndirəcək əlavələr bunlar olar:

- real cədvəl-owner siyahısı
- hər Bronze cədvəl üçün business glossary
- Airflow Variable-ların faktiki prod dəyərləri
- Tableau dashboard-ların konkret istifadə xəritəsi
- data quality qaydaları və SLA/SLO bölməsi

Hazırkı versiya isə repositoridə görünən BMIS texniki axınını başdan sona izah edən əsas referans sənəd kimi istifadə oluna bilər.
