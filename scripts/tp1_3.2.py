#Gabriel da Silva Freitas; Gabriel Luciano Nunes; Guilherme Silveira Duarte

import psycopg2

def drop_table(conn,cur): #função temporária para apagar a tabela (REMOVER)
	cur.execute('DROP TABLE Product CASCADE;')
	cur.execute('DROP TABLE Comments;')
	cur.execute('DROP TABLE Similars;')
	cur.execute('DROP TABLE Category;')

def remove_espaco_inicial(str_in):
	return str_in[1:]
	
def rewiew(teste):
	valores = []
	for termo in teste:
		if termo.replace('.','',1).isdigit():
			valores.append(termo)
	total = valores[0]
	#print(total)
	downloaded = valores[1]
	#print(downloaded)
	rating = valores[2]
	#print(rating)
	return (total, downloaded,rating)
	
def remove_espaco_vetor(frase):
	saida = []
	for analiza in frase:
		if (analiza != ''):
			saida.append(analiza)
	return saida
	
def povoa_tabela2(conn,cur,nome_arq):#insere valores do arquivo na tabela
	with open("entrada.txt","r") as arq:
		ident = ""
		#ASIN = ""
		title = ""
		grupo = ""
		salesrank = ""
		total = ""
		downloaded = ""
		rating = ""
		cutomer= ""
		date = ""
		votes = ""
		helpful = ""
		similares = []
		
		contador_product = 0
		contador_comentario = 1
		contador_categoria = 1
		contador_similar = 1
		linha = arq.readlines()
		for frase in linha: ## SALVA NA TABELA PRODUCT
			if "Id:" in frase and not "title" in frase and not "group" in frase and not "Id:   0" in frase:
				ident = frase.split(':')[1].replace('\n','').replace(' ','')
				print("Salvando na tabela PRODUCT o produto de ID: ",ident)
				contador_product+=1

				#print("ID::::",ident)
			if "title:" in frase:
				title = remove_espaco_inicial(frase.split(':')[1].replace('\n',''))
				#print("TITLE::::",title)
				contador_product+=1
			if "group:" in frase:
				grupo = frase.split(':')[1].replace('\n','').replace(' ','')
				#print("GRUPO::::",grupo)
				contador_product+=1
			if "salesrank:" in frase:
				salesrank = frase.split(':')[1].replace('\n','').replace(' ','')
				#print("SALESRANK::::",salesrank)
				contador_product+=1
			if "reviews:" in frase:
				auxiliar = frase[:-1].split(' ')
				total, downloaded, rating = rewiew(auxiliar)
				contador_product+=1
				
			if (contador_product == 5): #insere dados na tabela
				args = [ident,title,grupo,salesrank,downloaded,rating]
				#print(args)
				insere_sql = "INSERT INTO Product VALUES (%s,%s,%s,%s,%s,%s);"
				cur.execute(insere_sql,args)
				contador_product = 0
		
		for frase in linha: ####### SALVA NA TABELA COMMENTS
			#print(linha)
			if "Id:" in frase and not "title" in frase and not "group" in frase:
				ident = frase.split(':')[1].replace('\n','').replace(' ','')
				print("Salvando na tabela COMMENTS o comentarios do produto de ID:", ident)
			if "cutomer:" in frase and not "title" in frase:
				auxiliar = remove_espaco_vetor(frase[:-1].split(' '))
				date = auxiliar[0]
				cutomer = auxiliar[2]
				rating = auxiliar[4]
				votes = auxiliar[6]
				helpful = auxiliar[8]
				args = [contador_comentario,ident,cutomer,date,rating,votes,helpful]
				insere_sql = "INSERT INTO Comments VALUES (%s,%s,%s,%s,%s,%s,%s);"
				cur.execute(insere_sql,args)
				contador_comentario+=1	
				
		for frase in linha: ####### SALVA NA TABELA SIMILARS
			if "Id:" in frase and not "title" in frase and not "group" in frase:
				ident = frase.split(':')[1].replace('\n','').replace(' ','')
				print("Salvando na tabela SIMILARS os similares do produto de ID:", ident)
			if "similar:" in frase and not "title" in frase:
				auxiliar = remove_espaco_vetor(frase[:-1].split(' '))
				insere_sql = "INSERT INTO Similars VALUES (%s,%s,%s);"
				for i in range(2,len(auxiliar)):
					args = [contador_similar,ident,auxiliar[i]]
					contador_similar+=1
					cur.execute(insere_sql,args)
					#print(args)
						
		for i in range(0,len(linha)): ### SALVA NA TABELA CATEGORY
			frase = linha[i]
			if "Id:" in frase and not "title" in frase and not "group" in frase:
				ident = frase.split(':')[1].replace('\n','').replace(' ','')
				print("Salvando na tabela CATEGORY as categorias do produto de ID:",ident)
				
			if not "title:" in frase and "categories" in frase:
				num_cat = frase.split(':')[1].replace('\n','').replace(' ','') #pega a quantidade de categorias
				lista_id_categoria = []
				tuplas_categoria = []
				for j in range(i+1, i+int(num_cat)+1):
					categorias = linha[j].split('|')
					#print(categorias)
					#print("CATEGORIAS:",categorias)
					for subcategorias in categorias:
						if (subcategorias  != "   "):
							parte_subcategoria = subcategorias.split('[')
							if len(parte_subcategoria) > 2:
								for i in range(1,len(parte_subcategoria)-1):
									parte_subcategoria[i] = '[' + parte_subcategoria[i]
							id_categoria = parte_subcategoria[-1][:-1]
							nome_categoria = ''.join(parte_subcategoria[0:-1])
							if '[' in id_categoria:
								id_categoria = id_categoria.replace('[','')
							if ']' in id_categoria:
								id_categoria = id_categoria.replace(']','')
							#print(nome_categoria,id_categoria)
							if len(nome_categoria) == 0:
								nome = 'NULL'
							if id_categoria not in lista_id_categoria:
								lista_id_categoria.append(id_categoria)
								tuplas_categoria.append(("{}".format(id_categoria), "{}".format(ident), "{}".format(nome_categoria)))
							contador_categoria+=1
				insere_sql = "INSERT INTO Category(id_category, id_Product_Category, name_category) VALUES(%s,%s,%s);"
				for tupla in tuplas_categoria:
					print(tupla)
					args = [tupla[0], tupla[1], tupla[2]]
					cur.execute(insere_sql,args)
				tuplas_categoria = []

				
		arq.close()

def cria_relacao(conn,cur): #cria tabela
	try:
		cur.execute("""CREATE TABLE Product (
						id INTEGER,
						Title VARCHAR(1500),
						Group_Product VARCHAR(40),
						Salesrank INTEGER,
						Downloaded INTEGER,
						Avg_rating FLOAT,
						PRIMARY KEY(id));""")
						
		cur.execute("""CREATE TABLE Comments (
						id_comment integer,
						id_Product_Comment INTEGER,
						Cutomer VARCHAR(15),
						Date_comment DATE,
						rating INTEGER,
						votes INTEGER,
						helpful INTEGER,
						PRIMARY KEY(id_comment),
						FOREIGN KEY(id_Product_Comment) REFERENCES Product(id) ON UPDATE CASCADE);""")
				
		cur.execute("""CREATE TABLE Similars(
					id_relation_simility INTEGER,
					id_Product INTEGER,
					Product_Similar varchar(10),
					PRIMARY KEY(id_relation_simility),
					FOREIGN KEY(id_Product) REFERENCES Product(id) ON UPDATE CASCADE);""")
					
		cur.execute("""CREATE TABLE Category(
						id_Tupla_Category SERIAL,		-- id da tupla da Categoria
						id_category INTEGER,			-- id da categoria
						id_Product_Category INTEGER, 	-- id do produto da categoria
						name_category VARCHAR(150),		-- nome da categoria
						PRIMARY KEY(id_Tupla_Category),
						FOREIGN KEY(id_Product_Category) REFERENCES Product(id) ON UPDATE CASCADE);""")
					
		print("tabela criada com sucesso!")
	except:
		print("falha ao criar relação!")
		#ASIN_Product char(10),
		#ASIN_Similar char(10),
		
def pesquisa(): #pesquisa na tabela
	flag = 0
	try:
		cur.execute('SELECT * FROM produto')
		flag = 1
	except:
		print("Erro ao selecionar elementos");
	if (flag==1):
		print("mostrando valores de produto")
		for c in cur:
			print(c)

def fecha(conn,cur): #fecha a tabela
	conn.commit()
	cur.close()
	conn.close()
	
def main():
	
	nome = input("Digite o nome do usuário: ")
	senha = input("Digite a senha: ")
	nome_arq = input("Digite o nome do arquivo:")
	try: #tenta conectar
		conn = psycopg2.connect(host="localhost",database="postgres",user=nome,password=senha)
		cur = conn.cursor()
	except:
		print("Erro ao conectar ao banco de dados")
	
	drop_table(conn,cur)
	cria_relacao(conn,cur)
	povoa_tabela2(conn,cur,nome_arq)
	#pesquisa()
	fecha(conn,cur)
	
if __name__ == '__main__':
	main()
