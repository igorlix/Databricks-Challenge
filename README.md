# Databricks Challenge- Data Engineer
  Esse repositório contém a resolução do desafio de Engenharia de Dados utilizando a plataforma Databricks. O desafio consiste em ingerir uma dataset público, tratar os dados e agregar as informações de forma otimizada.

Link do dataset: https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric

## Considerações Iniciais
- Em relação ao ambiente, como proposto, utilizei a versão gratuita do Databricks. Apesar que algumas limitações, é uma plataforma muito completa para a realização da atividade.
- Sobre repositório, exportei diretamante da plataforma os notebooks utilizados. Adicionei comentários gerais, específicos e eventuais decisões técnicas em cada um deles, mas também compilei essas informações no relatório final.
- Tive algumas dúvidas durante o projeto, as quais também cito no relatório final.
- Infelizmente não consegui salvar as saídas de cada notebook no Github, o Databricks menciona que isso requer permissões de administrador, que são limitadas na versão gratuita, talvez essa seja a causa. Nesse caso, adicionei os arquivos em HTML, quue após baixados podem ser visualizadas com suas respectivas saídas.
  
## Guia de Execução
  Os notebooks exportados para esse repositório contemplam as 3 camadas: Bronze, Prata e Ouro. Para visualizar e interagir com as camadas e suas respectivas saídas é necessário baixar os seguintes arquivos (motivo citado em considerações iniciais).
  - [Camada Bronze](Desafio/HTML/1-Camada_Bronze.html?accessType=DOWNLOAD)
  - [Camada Silver](Desafio/HTML/2-Camada_Silver.html?raw=true)
  - [Camada Gold](Desafio/HTML/3-Camada_Gold.html?raw=true)
  - [Relatório Exploratório](Desafio/HTML/4-Relatório_Exploratório.html?raw=true)
  
 Outra opção é clonar esse repositório diretamante para Databricks. Nesse caso é necessário configurar o catálogo, adicionar o CSV e criar a tabela SQL para a camada bronze. 

### Jobs
  Para simular a ingestão diária criei um jobs simples que executa o notebook das camadas bronze, silver e gold em sequência a cada 24 horas. A condição de execução é o sucesso da camada anterior.
  (imagem)
  
## Relatório Final
  Compilei as principais informações, decisões técnicas, dúvidas e o funcionamento geral da solução neste relatório:

  (relatorio)


  
