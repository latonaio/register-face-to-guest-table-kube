# register-face-to-guest-table-kube 
## 概要 
register-face-to-guest-table-kube は、カンバン(=メッセージJSONファイル)経由で受け取ったゲストID、顧客の顔画像ファイルのパス、AzureFaceID、AzureFaceAPIにより判定された性別と年齢情報を、SQLのデータベースに書き込むマイクロサービスです。  
register-face-to-guest-table-kube は、AzureFaceAPIで顔認証された(過去にされたことのある)顧客顔情報をDBに更新するために用いられます。    
register-face-to-guest-table-kube は、エッジアプリケーションでの用途にフォーカスしていますが、クラウド側で利用することもできます。  

## 前提条件  
・(エッジ)アプリケーションサイドでの顧客情報の維持管理    
・AzureFaceAPIによる顔認証  
・(エッジ)アプリケーションサイドでの顔認証情報の維持管理    

## SQLの設定 
・顧客(=ゲストID)情報、顔認証情報を維持管理するための、SQLスキーマがセットアップされています  

## I/O
#### Input  
入力データのJSONフォーマットは、sample.jsonにある通り、次の様式です。  
例えばOMOTE-Bakoアプリケーションでは、前工程のマイクロサービス register-an-image-to-face-api-kube により、当該JSONファイルが出力されます。
```
{
    "result": true,
    "filepath": "/var/lib/aion/Data/direct-next-service_1/634173065679.jpg",
    "guest_id": 1,
    "face_id_azure": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "attributes": {
        "gender": "male",
        "age": "37.0"
    }
}
```
  
#### Output  
SQLの各カラムに下記の情報が書き込まれます  
1. ゲストID(guest_id)    
(エッジ)アプリケーションの顧客ID        
2. 顔画像ファイルのパス(filepath)      
顔画像ファイルのパス      
3. AzureFaceID(face_id_azure)      
AzureFaceAPIのFaceID    
4. 顔画像の属性情報      
AzureFaceAPIの返り値としての性別・年齢情報        

## Getting Started　　
1.下記コマンドでDockerイメージを作成します。  　　
```
make docker-build
```
2.aion-service-definitions/services.yml に設定を記載し、AionCore経由でKubernetesコンテナを起動します。    
services.ymlへの記載例：     
```
register-face-to-guest-table-kube:
  startup: yes
  always: yes
  scale: 1
  env:
    MYSQL_USER: XXXXXX
    MYSQL_HOST: XXXXXX
    MYSQL_PASSWORD: XXXXXX
    RABBITMQ_URL: amqp://username:password@rabbitmq:5672/virtualhost
    QUEUE_ORIGIN: register-face-to-guest-table-kube-queue
```
## Flowchart  
![img](doc/flowchart.png)