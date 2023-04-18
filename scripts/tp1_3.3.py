#Gabriel da Silva Freitas; Gabriel Luciano Nunes; Guilherme Silveira Duarte

import psycopg2

try: #tenta conectar
	conn = psycopg2.connect(host="localhost",database="postgres",user="postgres",password="postgres")
	cur = conn.cursor()
except:
	print("Erro ao conectar ao banco de dados")
	
def consulta_a():
	produto = input("Consulta a -----> Digite o id do produto: ")
	
	cur.execute("SELECT id FROM Product WHERE id = %s ",(produto,))
	produto_id = cur.fetchone()[0]

	cur.execute("SELECT * FROM Comments WHERE id_Product_Comment = %s ORDER BY helpful DESC, rating DESC LIMIT 5",(produto_id,))
	mais_avaliados = cur.fetchall()
	
	cur.execute("SELECT * FROM Comments WHERE id_Product_Comment = %s ORDER BY helpful DESC, rating ASC LIMIT 5",(produto_id,))
	menos_avaliados = cur.fetchall()

	consulta = cur.fetchall()
	
	print("\n----- Mais avaliados e mais úteis ----\n")
	for consulta_um in mais_avaliados:
		print(consulta_um)
	print("\n---- Menos avaliados e mais úteis ----\n")
	for consulta_dois in menos_avaliados:
		print(consulta_dois)



def consulta_c():
 # Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
 produto = input("\nConsulta c -----> Digite o id do produto: ")
 
 cur.execute("SELECT id FROM Product WHERE id = %s ",(produto,))
 produto_id = cur.fetchone()[0]

 cur.execute("SELECT date_comment,rating FROM Comments WHERE id_Product_Comment = %s ",(produto_id,))
 result_comments = cur.fetchall()
 
 vetor_data = []
 for tupla in result_comments:
  vetor_data.append(tupla[0])
 
 resultado_media_data = [] 
 for data in vetor_data:
  cur.execute("SELECT DISTINCT date_comment,AVG(rating) FROM Comments WHERE id_product_Comment = %s AND date_comment = %s GROUP BY date_comment ", (produto_id,data))
  resultado_comando = cur.fetchall()[0]
  if not resultado_comando in resultado_media_data:
   resultado_media_data.append(resultado_comando)
 
 if (len(resultado_media_data)==0):
  print("Esse produto não tem avaliações")
 else:
  print("Evolução Diária das médias de avaliação do produto ao longo do tempo:")
  for res in resultado_media_data:
   data_avaliacao = "Data da avaliação: "+str(res[0]) 
   avg_avaliacao_data = f"\nMédia da avaliação nessa data: {res[1]:,.2f}"
   print(data_avaliacao,avg_avaliacao_data)
   print(res)

def consulta_d():
	
	cur.execute("""SELECT Group_Product, Title, Salesrank
    				FROM (
        			SELECT Group_Product, Title, Salesrank, 
               		ROW_NUMBER() OVER (PARTITION BY Group_Product ORDER BY Salesrank ASC) AS top_dez
        			FROM Product WHERE Salesrank >=1
    				) sub
    				WHERE top_dez <= 10
    				ORDER BY Group_Product, Salesrank ASC;""")
	consulta = cur.fetchall()
	print("\n---- Consulta d ----\n")
	for resultado in consulta:
		print(resultado)

def consulta_e():
	#Listar os 10 produtos com a maior média de avaliações úteis positivas por produto


	cur.execute("""
	SELECT product.id, product.title, AVG(comments.helpful) AS avg_helpful
    FROM product
    JOIN comments ON product.id = comments.id_product_comment
    GROUP BY product.id, product.title
    ORDER BY avg_helpful DESC
    LIMIT 10;
	""")
	resultado = cur.fetchall()
	print("\n---- Consulta e ----\n")
	print("id_product | title_product | avg_helpful")
	print("------------------------------------------")
	for tupla in resultado:
		print("{} | {} | {}".format(tupla[0], tupla[1], tupla[2]))
	
def consulta_f():
	#Listar os 10 produtos com a maior média de avaliações úteis positivas por produto
	cur.execute("""
	SELECT category.id_category, category.name_category, AVG(comments.helpful) AS avg_helpful
	FROM category
	JOIN comments ON category.id_product_category = comments.id_product_comment
	GROUP BY category.id_category, category.name_category
	ORDER BY avg_helpful DESC
	LIMIT 5;
	""")
	resultado = cur.fetchall()
	print("\n---- Consulta f ----\n")
	print("id_category | name_category | avg_helpful_category")
	print("------------------------------------------")
	for tupla in resultado:
		print("{} | {} | {}".format(tupla[0], tupla[1], tupla[2]))

def consulta_g():
 # Listar os 10 clientes que mais fizeram comentários por grupo de produto 
 
 cur.execute('''SELECT p.Group_Product, c.Cutomer, COUNT(*) AS Num_Comments
     FROM Comments c
     JOIN Product p ON c.id_Product_Comment = p.id
     GROUP BY p.Group_Product, c.Cutomer
     ORDER BY p.Group_Product, Num_Comments DESC
     LIMIT 10;
     ''')
 resultado = cur.fetchall()
 print("\n---- Consulta g ----\n")
 print("10 clientes que mais fizeram comentários por grupo de produto \n")
 for tuplas in resultado:
  print("Grupo: ",tuplas[0],"Cliente: ",tuplas[1]," Qtde de Comentários: ",tuplas[2],"\n")

def fecha(): #fecha a tabela
	conn.commit()
	cur.close()
	conn.close()

def main():
	consulta_a()
	#consulta_b()
	consulta_c()
	consulta_d()
	consulta_e()
	consulta_f()
	consulta_g()


	fecha()
	
if __name__ == '__main__':
	main()
