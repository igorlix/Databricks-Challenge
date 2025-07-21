# Databricks Challenge- Data Engineer
  Esse repositório contém a resolução do desafio de Engenharia de Dados utilizando a plataforma Databricks. O desafio consiste em ingerir uma dataset público, tratar os dados e agregar as informações de forma otimizada.

Link do dataset: https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric

## Considerações Iniciais
- Em relação ao ambiente, como proposto, utilizei a versão gratuita do Databricks. Apesar que algumas limitações, é uma plataforma muito completa para a realização da atividade.
- Sobre repositório, exportei diretamante da plataforma os notebooks utilizados. Adicionei comentários gerais, específicos e eventuais decisões técnicas em cada um deles, mas também compilei essas informações no relatório final.
- Tive algumas dúvidas durante o projeto, as quais também cito no relatório final.
- Infelizmente não consegui salvar as saídas de cada notebook no Github, o Databricks menciona que isso requer permissões de administrador, que são limitadas na versão gratuita, talvez essa seja a causa. Nesse caso, adicionei os arquivos em HTML, que após baixados podem ser visualizadas com suas respectivas saídas.
  
## Guia de Execução
Os notebooks exportados para esse repositório contemplam as 3 camadas: Bronze, Prata e Ouro. Para visualizar e interagir com as camadas e suas respectivas saídas é necessário baixar os arquivos HTML ou clicar nos seguintes links: (motivo citado em considerações iniciais).
  - [Camada_Bronze](https://raw.githack.com/igorlix/Databricks-Challenge/ddd3ff41ac569f6b04d3e2830e3f1e7d5d6b86bd/Desafio/HTML/1-Camada_Bronze.html)
  - [Camada Silver](https://raw.githack.com/igorlix/Databricks-Challenge/refs/heads/main/Desafio/HTML/2-Camada_Silver.html)
  - [Camada Gold](https://raw.githack.com/igorlix/Databricks-Challenge/refs/heads/main/Desafio/HTML/3-Camada_Gold.html)
  - [Relatório Exploratório](https://raw.githack.com/igorlix/Databricks-Challenge/refs/heads/main/Desafio/HTML/4-Relat%C3%B3rio_Explorat%C3%B3rio.html)

Também é possível acessar pelos notebooks, mesmo que sem visualizar a saída deles:
- [Notebooks](https://github.com/igorlix/Databricks-Challenge/tree/ec4c11b8b51d16afef6813974ff6f63b0c4f16fb/Desafio/Notebooks)

  
 Outra opção é clonar esse repositório diretamante para o Databricks. Nesse caso é necessário configurar o catálogo, adicione o CSV para um volume nomeado "arquivos_brutos" e crie a tabela SQL com o nome "fire_incidents_bronze" para a camada bronze (as outras tabelas são geradas ao rodar os notebooks).
 
 <img width="1714" height="380" alt="image" src="https://github.com/user-attachments/assets/c3d8b029-5ad4-4af5-afe7-09eee2c6f7a9" />
 
<img width="1707" height="763" alt="Captura de tela 2025-07-20 215216" src="https://github.com/user-attachments/assets/544fca30-f05e-4c07-a504-3c86d9d7d8e2" />

## Jobs
  Para simular a ingestão diária criei um jobs simples que executa o notebook das camadas bronze, silver e gold em sequência a cada 24 horas. A condição de execução é o sucesso da camada anterior.

  
<img width="1706" height="816" alt="Captura de tela 2025-07-20 214701" src="https://github.com/user-attachments/assets/755d60a5-894f-422a-b855-8d76f26688d9" />
<img width="1697" height="820" alt="Captura de tela 2025-07-20 214654" src="https://github.com/user-attachments/assets/bb0d8ee5-a4fc-4f1e-ae39-85b0bdc37650" />

  
## Relatório Final
  Compilei as principais informações, decisões técnicas, dúvidas e o funcionamento geral da solução neste relatório:
- [Relatório](https://github.com/igorlix/Databricks-Challenge/blob/151373531361e15df266159c82c8e3eda00b1ce6/Desafio/Relat%C3%B3rio_Final.md)

