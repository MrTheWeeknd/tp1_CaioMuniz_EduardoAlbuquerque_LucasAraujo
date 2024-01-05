import psycopg2
from tqdm import tqdm

def conecta_db():
    try:
        con = psycopg2.connect(host='localhost',
                               port='5432',
                               user='postgres',
                               password='lucas123',
                               database='Data1')
        return con, con.cursor()
    except (Exception, psycopg2.Error) as erro:
        print(f"Erro ao conectar ao banco de dados: {erro}")
        return None, None

def limpandoVetores(vetor):
    vetor = vetor.replace('\r', "")
    vetor = vetor.split("\n")
    vetor = list(filter(None, vetor))
    vetor.append("end")
    vetor = ', '.join(vetor)
    return vetor

def extrairArq():
    try:
        file = open("amazon1-meta.txt", 'r', encoding='utf-8')
        file.seek(82)  
        valores = file.read()  

        valores = valores.split("\nId") 

        produtos = []
        for i, valor in enumerate(valores):
            if i < 6000000:  
                valor = limpandoVetores(valor)

                print(f"Processando produto {i + 1}")

                try:
                    id_str = valor[valor.find(":   ") + 4:valor.find("ASIN")].replace('\r\n', "").strip(" ")
                    id_str = id_str.replace(",", "")

                    id = int(''.join(filter(str.isdigit, id_str)))

                    if "discontinued product" in valor:
                        asin = valor[valor.find("ASIN: ") + 7:valor.find("discontinued product")].replace('\r\n', "").strip(" ")
                        produtos.append((id, asin, "discontinued product"))
                    else:
                        asin = valor[valor.find("ASIN: ") + 7:valor.find("title: ")].replace('\r\n', "").strip(" ")
                        title = valor[valor.find("title: ") + 7:valor.find("group")].replace('\r\n', "").strip(" ")
                        group = valor[valor.find("group: ") + 7:valor.find("salesrank")].replace('\r\n', "").strip(" ")
                        salesrank = valor[valor.find("salesrank: ") + 11:valor.find("similar")].replace('\r\n', "").strip(" ")

                        similar = valor[valor.find("similar: ") + 9:valor.find("categories")].replace('\r\n', "").strip(" ")
                        categories = valor[valor.find("categories: ") + 12:valor.find("reviews")].replace('\r\n', "").strip(" ")
                        reviews = valor[valor.find("reviews: ") + 16:valor.find("end")].replace('\r\n', "").strip(" ")

                        additional_info = []
                        ratings_info = valor[valor.find("reviews:") + 16:valor.find("end")].replace('\r\n', "").strip(" ")
                        ratings_info = ratings_info.split(',')
                        for item in ratings_info:
                            item = item.strip()
                            if item:
                                additional_info.append(tuple(item.split(':')))
                        
                        produtos.append((id, asin, title, group, salesrank, similar, categories, reviews, additional_info))
                except Exception as e:
                    print(f"Erro ao processar produto {i + 1}: {e}")

        file.close()
        return produtos
    except Exception as e:
        print(f"Erro ao abrir ou ler o arquivo: {e}")
        return []

def inserirBD(sql, valores=None):
    con, cur = conecta_db()

    try:
        if valores:
            cur.execute(sql, valores)
        else:
            cur.execute(sql)

        con.commit()
        print("Inserção bem-sucedida!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro durante a inserção: {error}")
        con.rollback()
        return 1
    finally:
        cur.close()
        con.close()

def inserindoProduct(id, asin, title, group, salesrank, qtd_similares, reviews):
    if reviews:
        reviews_list = list(reviews)
        if reviews_list:
            review_total = int(reviews_list[0])
            avg_rating = float(reviews_list[5].replace(",", ""))

        inserirBD('''INSERT INTO public.tbl_produto (
                    produto_id, 
                    produto_asin, 
                    produto_title,
                    produto_salesrank,
                    produto_group) VALUES (%s, %s, %s, %s, %s);''', 
                    (id, asin, title, salesrank, group))

        produto_id = id
        
        inserirBD('''INSERT INTO public.tbl_comentarios (
                    produto_id,
                    dia,
                    cliente,
                    classificacao,
                    avg_rating,
                    votos,
                    util) VALUES (%s, %s, %s, %s, %s, %s, %s);''',
                    (produto_id, 'dia', 'cliente', 5, avg_rating, 10, 5))
        for i in range(qtd_similares):
            similar_asin = f"similar_asin_{i}"
            inserirBD('''INSERT INTO public.tbl_similares (
                        produto_id,
                        similar_asin) VALUES (%s, %s);''',
                        (produto_id, similar_asin))

def povoandoTabelas(produtos):
    for i in tqdm(range(len(produtos))): 
        produto = produtos[i] 
        id = int(produto[0]) 
        asin = produto[1].replace(",", "") 
        
        if(produto[2] == "discontinued product"): 
            inserirBD('''INSERT INTO public.product (
                Produto_Id , 
                Produto_IdAsin) VALUES (%d, '%s');''' % 
                (id, asin))
            
        if(produto[2] != "discontinued product"): 
            title = produto[2].replace(",", "")
            title = title.replace("'", "")
            
            group = produto[3].replace(",", "")
            salesrank = produto[4].replace(",", "")
            
            salesrank = int(salesrank)
            similares = produtos[i][5].split(" ")
            
            qtd_similares = list(filter(None, similares))
            if qtd_similares:
                qtd_similares = int(qtd_similares[0].replace(",", ""))
            else:
                qtd_similares = 0

            
            reviews = produtos[i][7].split(" ")
            reviews = list(filter(None, reviews))
            
            inserindoProduct(id, asin, title, group, salesrank, qtd_similares, reviews)

produtos = extrairArq()
con, cur = conecta_db()
povoandoTabelas(produtos)
con.commit()
cur.close()
con.close()
