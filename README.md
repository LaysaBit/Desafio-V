# Desafio-V
# Indicium , atividade Airflow

## Contexto

- Com base no banco de dados Northwind_small.sqlite, extrair dados da tabela com Airflow para gerar arquivo final_output.txt.

### Em 3 partes:
- Selecionar todas as linhas da tabela "Order" do banco Northwind_small.sqlite para um arquivo CSV ; 
- Fazer o merge de OrderDetail e "Order" por meio de uma dataframes do Python;  
  - Através disso, calcular a quantidade total que foi vendida para o Rio de Janeiro e armazenar esse valor num arquivo count.txt.
- Codificar o arquivo count.txt concatenado com uma variável my_email e armazenar isso no arquivo final_output.txt.

## Requerimentos
- Instalar Docker.

### O Airflow - Configuração

* Executar a instalação dos containers de dockers com os módulos necessários:

```
    docker-compose up --build
```

- Esse comando faz:
    - *1)* instala os requerimentos necessários em requirements.txt.
    - *2)* Cria as imagens dos containers no Docker e os inicializa.

### O Sevidor Apache Airflow deve ser acessado pelos:

```
Servidor = http://0.0.0.0:8083/
Usuário: Airflow
Senha = Airflow
```

- DAGS
    - Configura-se, automaticamente, a pasta DAGS dentro do /opt/airflow/dags.
    - Criar um arquivo Python dag.py dentro da pasta Dags, onde está toda a pipeline.
    - Dentro da pasta DAGS também está localizado o banco de dados utilizado para a atividade: Northwind_small.sqlite.

#### Informações Adicionais

- Antes de ligar a pipeline "Desafio-V", criar uma variável "my_email" conforme a imagem:
    - Admin -> Variables:
    <img src = https://user-images.githubusercontent.com/46203330/236343136-48536990-9db2-4d90-8f69-3f536172383d.png />

- Os arquivos "output_orders.csv", "count.txt" e "final_output.txt" foram/são salvos na pasta data.


## Desligar o programa
```
sudo docker-compose down
```
