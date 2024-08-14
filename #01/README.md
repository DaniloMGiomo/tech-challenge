repo: https://github.com/DaniloMGiomo/tech-challenge/tree/main/%2301

![image](https://github.com/user-attachments/assets/8df02962-d39d-468c-b3d9-7b317d398a2d)

<img width="719" alt="image" src="https://github.com/user-attachments/assets/1906b399-8a9d-45e2-97b5-ceb5175023b8">

Temos dois processos nesse projeto;
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


![image](https://github.com/user-attachments/assets/a627630e-3086-4520-987e-d046905edcf7)
a API e todos sub-codigos estão com suas respectivas dog-strings e comentários:
![image](https://github.com/user-attachments/assets/01aaa597-e209-41e4-8441-d84ce94d5e27)
![image](https://github.com/user-attachments/assets/0e9f56ee-9c6a-4592-98c2-0810d6dfae7c)
![image](https://github.com/user-attachments/assets/64b22e56-2cb6-436d-a053-0f709f970ea4)


![image](https://github.com/user-attachments/assets/4dad23f2-5618-4bf8-9a37-8383db52d5b1)
e está protegida pelo metodo de autenticação JWT:
<img width="719" alt="image" src="https://github.com/user-attachments/assets/9e2f1918-b611-40e4-bc2e-8f6222baa8d8">

- usuário: test
- senha: test

![image](https://github.com/user-attachments/assets/cb3a4c8d-c13d-4f3e-a42b-726e526c3e5b)
A aplicação está conteinerizada sendo necessário apenas executar o arquivo 'startup.sh'
está sendo executada nua maquina EC2 t2.small na AWS.
![image](https://github.com/user-attachments/assets/e7529391-8300-4b59-8a6d-e62418e4ad16)
o banco de dados será transferido pra um S3 para mair confiabilidade posteriormente


![image](https://github.com/user-attachments/assets/a96a1ba1-2896-40e7-a50c-b950726616b9)
sendo acessível pela url: http://52.206.210.53:8080/docs
