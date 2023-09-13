# Marketing APIs

Este repositório contém códigos utilizados para extração de dados de campanha de marketing de redes sociais.

A pasta *api* contém um arquivo .py pra cada rede social em específico no formato *api_{rede}*.
É utilizada uma planilha de controle para verificar se existe anúncio sendo veiculado em cada rede social bem como sua data de início e data de fim.

A partir dessa planilha, o código escrito no arquivo *main.py* chama os respectivos códigos de cada rede social gerando uma tabela para cada rede e também uma tabela final com todas os dados da campanha, incluindo dados do Google Analytics.
