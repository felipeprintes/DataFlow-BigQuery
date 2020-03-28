# DataFlow-BigQuery
O objetivo é mostrar como realizar ingestão de dados no BigQuery, utilizando Dataflow como ferramenta de ETL. 
Os dados que são utilizados, serão dados do novo coronavírus que podem ser baixados diretamente do kaggle

# First things first
### O que é Dataflow?? 
Dataflow é uma ferramenta de ETL com suporte para processamento de dados tanto em batch quanto em streaming.
Nesse exemplo, o processamento é feito em batch. 
O Dataflow tem algumas considerações bem bacanas a serem levadas em consideração 
1. Baseado em Apache Beam, o que garante a portabilidade do meu código
2. Serviço totalmente gerenciado. Não preciso me preocupar com provisionamento de 
infraestrutura, o Dataflow cuida disso pra mim.
3. Escalonamento horizontal automático
4. Pagamento apenas pelo momento que o job está executando 

# O que preciso pra executar o código? 
Como falei, o Dataflow é baseado no Apache Beam, que é open source, o que também quer dizer que você 
pode executa-lo na sua máquina local. Porém o objetivo aqui é realizar a ingestão de dados no BigQuery 
e pra isso, é necessário ter uma conta no GCP (Google Cloud Platform). 

Aí, você me pergunta __"ué, mas não é pago??"__
E eu te respondo: Sim, é pago! __Porém__, ao fazer sua conta, o google da um crédito de $300 pra
que as pessoas possam realizar testes das feramentas por um período de 1 ano, aproveitem.

# Ferramentas para atividade
- Google Cloud Storage
- DataFlow - Python
- BigQuery

# Como executar
Crie seu ambiente e instale os pacote
- ```virtualenv myenv```
- ```pip install -r requirements.txt```

