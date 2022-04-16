## ТЕСТОВОЕ ЗАДАНИЕ НА ВАКАНСИЮ [Yandex](https://yandex.ru/jobs/vacancies/junior-%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%87%D0%B8%D0%BA-%D0%B2-%D1%82%D0%B5%D1%85%D0%B3%D1%80%D1%83%D0%BF%D0%BF%D1%83-%D0%BF%D0%BE%D0%B4%D0%B4%D0%B5%D1%80%D0%B6%D0%BA%D0%B8-%D0%BC%D0%B5%D0%B4%D0%B8%D0%B0%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D0%BE%D0%B2-7967#form):

### Задание 1
```
Есть набор данных о стоимости недвижимости в Великобритании - .csv файл (скачать можно по ссылке
https://disk.yandex.ru/d/ZTnv3LiUeqEK5A, описание столбцов тут https://www.gov.uk/guidance/about-the-price-paid-data)
нужно написать программу, которая сформирует файл, в котором будет перечислена вся недвижимость, проданная больше 1-го раза.
Программа должна потреблять как можно меньше вычислительных ресурсов. Рассмотреть случаи экономии памяти и процессорного времени.

```
 

### Требования
* Программа должна потреблять как можно меньше вычислительных ресурсов
* Рассмотреть случаи экономии памяти и процессорного времени


### Данные
* Данные для тестового задания представлены в формате [CSV](https://disk.yandex.ru/d/ZTnv3LiUeqEK5A), размер файла более 4Gb.

### Выбор программного обеспечения для решения данного типа задач
   Для работы с *CSV* Data существует несколько пакетов для анализа, среди них:
    
   * [Pandas](https://pandas.pydata.org/) не подошел по нескольким причинам 
        * Низкая производительность при загрузке/экспорте базы данных и файлов.
        * Сложные групповые операции неудобны и медленны
       
        
        С помощью данного инструмента не удалось получить результат по исходным данным, не хватило памяти 
        
   * Выбрал [Vaex](https://github.com/vaexio/vaex) т.к. использует отображение памяти, политику нулевого копирования памяти и ленивые вычисления для лучшей производительности (без потери памяти). 

### Алгоритм решения
   Для однозначной идентификации списка недвижимости необходимо сформировать критерии отбора:
   
   1. Определим группу полей, однозначно определяющих недвижимость:
        * paon, street, locality, towncity, district, country  
        [описание полей](https://www.gov.uk/guidance/about-the-price-paid-data) 
    
   2. По критериям данной группы посчитаем количество раз попадания в данную группу, ***если > 2***, то данная недвижимость была продана ***более 1-го раза***
   3. Пример реализация данного алгоритма на SQL:
      ``` 
      select p.* from pp_data as p
         inner join (
             select paon, street, locality, towncity, district, country
                 from pp_data
             group by paon, street, locality, towncity, district, country
             having count(paon) > 2        
         ) as  g 
         on p.paon = g.paon and p.street = g.street
         and p.locality = g.locality and p.towncity = g.towncity
         and p.district = g.district and p.country = g.country
      order by p.paon, p.transfer_date
      ```
   4. Реализация данного алгоритма на *Vaex* представлена в файле process.py  

### Первый запуск process.py 
    
   * При первом запуске будет загружен файл pp_complete.csv с Yandex Disk
   * Процесс анализа данных запускается в режиме отладки DEBUG=True
   * Для запуска полного анализа данных необходимо установить флаг DEBUG=False в файле 
     *.src/common/.env* и удалить файл с сформированными тестовыми данными *.data/processed/pp-complete.csv.hdf5*
   * Результат работы будет сохранен в файл *.data/final/final.csv*      

## Tools used in this project
* [Poetry](https://towardsdatascience.com/how-to-effortlessly-publish-your-python-package-to-pypi-using-poetry-44b305362f9f): Dependency management - [article](https://towardsdatascience.com/how-to-effortlessly-publish-your-python-package-to-pypi-using-poetry-44b305362f9f)
* [hydra](https://hydra.cc/): Manage configuration files - [article](https://towardsdatascience.com/introduction-to-hydra-cc-a-powerful-framework-to-configure-your-data-science-projects-ed65713a53c6)
* [pre-commit plugins](https://pre-commit.com/): Automate code reviewing formatting  - [article](https://towardsdatascience.com/4-pre-commit-plugins-to-automate-code-reviewing-and-formatting-in-python-c80c6d2e9f5?sk=2388804fb174d667ee5b680be22b8b1f)
* [DVC](https://dvc.org/): Data version control - [article](https://towardsdatascience.com/introduction-to-dvc-data-version-control-tool-for-machine-learning-projects-7cb49c229fe0)
* [pdoc](https://github.com/pdoc3/pdoc): Automatically create an API documentation for your project

## Project structure
```bash
.
├── config                      
│   ├── main.yaml                   # Main configuration file
│   ├── model                       # Configurations for training model
│   │   ├── model1.yaml             # First variation of parameters to train model
│   │   └── model2.yaml             # Second variation of parameters to train model
│   └── process                     # Configurations for processing data
│       ├── process1.yaml           # First variation of parameters to process data
│       └── process2.yaml           # Second variation of parameters to process data
├── data            
│   ├── final                       # data after training the model
│   ├── processed                   # data after processing
│   ├── raw                         # raw data
│   └── raw.dvc                     # DVC file of data/raw
├── docs                            # documentation for your project
├── dvc.yaml                        # DVC pipeline
├── .flake8                         # configuration for flake8 - a Python formatter tool
├── .gitignore                      # ignore files that cannot commit to Git
├── Makefile                        # store useful commands to set up the environment
├── models                          # store models
├── notebooks                       # store notebooks
├── .pre-commit-config.yaml         # configurations for pre-commit
├── pyproject.toml                  # dependencies for poetry
├── README.md                       # describe your project
├── src                             # store source code
│   ├── __init__.py                 # make src a Python module 
│   ├── process.py                  # process data before training model
│   └── train_model.py              # train model
└── tests                           # store tests
    ├── __init__.py                 # make tests a Python module 
    ├── test_process.py             # test functions for process.py
    └── test_train_model.py         # test functions for train_model.py
```

## Set up the environment
1. Install [Poetry](https://python-poetry.org/docs/#installation)
2. Set up the environment:
```bash
make activate
make setup
```

## Install new packages
To install new PyPI packages, run:
```bash
poetry add <package-name>
```

## Run the entire pipeline
To run the entire pipeline, type:
```bash
dvc repo
```

## Version your data
Read [this article](https://towardsdatascience.com/introduction-to-dvc-data-version-control-tool-for-machine-learning-projects-7cb49c229fe0) on how to use DVC to version your data.

Basically, you start with setting up a remote storage. The remote storage is where your data is stored. You can store your data on DagsHub, Google Drive, Amazon S3, Azure Blob Storage, Google Cloud Storage, Aliyun OSS, SSH, HDFS, and HTTP.

```bash
dvc remote add -d remote <REMOTE-URL>
```

Commit the config file:
```bash
git commit .dvc/config -m "Configure remote storage"
```

Push the data to remote storage:
```bash
dvc push 
```

Add and push all changes to Git:
```bash
git add .
git commit -m 'commit-message'
git push origin <branch>
```

# Auto-generate API documentation

To auto-generate API document for your project, run:

```bash
make docs
```
