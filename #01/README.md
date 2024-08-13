<img width="719" alt="image" src="https://github.com/user-attachments/assets/1906b399-8a9d-45e2-97b5-ceb5175023b8">Temos dois processos nesse projeto;

Processo de descoberta de arquivos para criação do 'banco de dados' em arquivos parquet:
![image](https://github.com/user-attachments/assets/a3dee7c6-12f5-41b7-8e5d-71fe791ab531)

  Este processo consiste em 'scrapar' todas paginas do site e coletar todos dados referente às paginas encontradas, utilizando a API de back-end do site

Processo da API:
![image](https://github.com/user-attachments/assets/b97d1722-602f-4dc1-9d57-5b246b8557a5)

  1. Quando o usuário seleciona o dado via API busca os dados no 'banco.parquet'
  2. Havendo dado ele retorna os dados sem necessidade de coletar no site da embrapa
  3. Não tendo os dados ele busca no site da embrapa
  4. Se encontrado o dado ele appenda no nosso 'banco.parquet' e retorna os dados na API
  5. Não encontrando dado na fonte ele retorna mensagem de erro:
     ![image](https://github.com/user-attachments/assets/5958e4ec-a491-487b-89aa-0f68caab4999)

endpoints:
![image](https://github.com/user-attachments/assets/4ab4564f-3f27-48e1-879e-45131761783a)

  Foi construido um endpoint para cada processo do site da embrapa:
    get_producao:
![image](https://github.com/user-attachments/assets/8b31ff1a-46b9-454f-a5ad-b4814638379b)

    get_processamento:
![image](https://github.com/user-attachments/assets/07e9995d-4375-444d-a5e6-82e24393dc3f)

    get_comercializacao:
![image](https://github.com/user-attachments/assets/73bcd8cb-1f70-4374-8ca4-b9d4832cd616)

    get_importacao:
![image](https://github.com/user-attachments/assets/3cb851e4-0263-49ed-a4bd-8daa9fc7fc4f)

    get_exportacao:
![image](https://github.com/user-attachments/assets/2480d8b7-1a2d-4878-88ce-b4069d87f39d)

A aplicação está conteinerizada sendo necessário apenas executar o arquivo 'startup.sh'
está sendo executada nua maquina EC2 t2.small na AWS, sendo acessível pela url: https://52.206.210.53:8080/docs

e está protegida pelo metodo de autenticação JWT:
<img width="719" alt="image" src="https://github.com/user-attachments/assets/9e2f1918-b611-40e4-bc2e-8f6222baa8d8">

usuário: test
senha: test

