# Итоговый проект

### Описание
Репозиторий предназначен для сдачи итогового проекта.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-final` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/Yandex-Practicum/de-project-final`
3. Перейдите в директорию с проектом: 
	* `cd de-project-final`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GitHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.

docker run -d -p 8998:8998 -p 8280:8280 -p 15432:5432 --name=de-final-prj-local cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest


### Комментариии к реализации

1. Шаг 3. Пункт 6. Проверьте, что ежедневные данные инкрементами загружены в схему *__STAGING хранилища.
	Не получилось выполнить 
	`SELECT DROP_PARTITIONS('STV2023060656__STAGING.{table}', '{execut_date}', '{execut_date}', 'true');`
	Получаю ошибку:
	`vertica_python.errors.InsufficientResources: Severity: ERROR, Message: Memory budget is not enough for` 
	`partition projection operation with group expression`
	`Current resource pool: user_1, memory budget: 46357 KB, memory required: 65472 KB`
	Для очистки использую:
	`DELETE FROM STV2023060656__STAGING.{table}`
    `WHERE {column_dt}::date = '{execut_date}';`

2. 

