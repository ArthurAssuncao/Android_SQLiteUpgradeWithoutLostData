package dao;

import java.util.ArrayList;
import java.util.List;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Classe que implementa os métodos para manipulação de tabelas do banco de dados (como 
 * criação e alteração de tabelas).
 * 
 * @see android.database.sqlite.SQLiteOpenHelper;
 * 
 * @author Arthur Assunção
 * @license Creative Commons3 <http://creativecommons.org/licenses/by-sa/3.0/deed.pt_BR>
 */
public class SQLHelper extends SQLiteOpenHelper {

	private String[] tabelas; // vetor com os nomes das tabelas do banco de dados.
	private String[] create; // vetor com o comando para criação das tabelas (CREATE TABLE tabela(campos)).
	private String[] tabelasRemovidas; // vetor com as tabelas que foram removidas do banco.
	private final String sufixoTabelaTemp = "_temp"; // sufixo utilizado pela tabela temporária.
	private static final int MAX_REGISTROS_TRANSACAO = 500; // máximo de registros por transação
	private static int REGISTROS_TRANSACAO;

	/** 
	 * Construtor sobrecarregado da classe SQLHelper.
	 * @param context recebe o contexto da acticty atual.
	 * @param name recebe o nome do banco de dados.
	 * @param version recebe a versão do banco de dados.
	 * @param tabelas recebe um vetor com os nomes das tabelas.
	 * @param create recebe um vetor com os comandos de criação das tabelas.
	 * @param tabelasRemovidas recebe um vetor com o nome das tabelas removidas.
	 */
	public SQLHelper(Context context, String name, int version, String[] tabelas, String[] create, String[] tabelasRemovidas) {
		super(context, name, null, version);

		this.tabelas = tabelas;
		this.create = create;
		this.tabelasRemovidas = tabelasRemovidas; 
	}

	/** 
	 * Método sobrecarregado onCreate(SQLiteDatabase db) para a criação das tabelas no banco de dados.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * 
	 * @see android.database.sqlite.SQLiteOpenHelper#onCreate(android.database.sqlite.SQLiteDatabase)
	 */
	@Override
	public void onCreate(SQLiteDatabase db) {
		db.beginTransaction();
		// apaga as tabelas que forma removidas do banco
		for(String tabelaRemovida : tabelasRemovidas){
			apagaTabela(db, tabelaRemovida);
		}
		
		// cria as tabelas
		for(int i = 0; i < create.length; i++){
			db.execSQL(create[i]);
		}
		db.setTransactionSuccessful();
		db.endTransaction();
	}

	/** 
	 * Método sobrecarregado onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) para 
	 * a atualização do banco de dados para uma nova versão.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param oldVersion recebe um <code>int</code> com a versão amtiga do banco de dados.
	 * @param newVersion recebe um <code>int</code> com a versão nova do banco de dados.
	 * 
	 * @see android.database.sqlite.SQLiteOpenHelper#onUpgrade(android.database.sqlite.SQLiteDatabase, int, int)
	 */
	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		db.beginTransaction();
		recriaBanco(db, oldVersion, newVersion);
		db.setTransactionSuccessful();
		db.endTransaction();
	}

	/** 
	 * Método sobrecarregado onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) para 
	 * a recriação do banco de dados para uma versão antiga.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param oldVersion recebe um <code>int</code> com a versão amtiga do banco de dados.
	 * @param newVersion recebe um <code>int</code> com a versão nova do banco de dados.
	 * 
	 * @see android.database.sqlite.SQLiteOpenHelper#onUpgrade(android.database.sqlite.SQLiteDatabase, int, int)
	 */
	@Override
	public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		db.beginTransaction();
		recriaBanco(db, oldVersion, newVersion);
		db.setTransactionSuccessful();
		db.endTransaction();
	}

	/** 
	 * Método para a recriação do banco de dados com os dados existentes.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param oldVersion recebe um <code>int</code> com a versão amtiga do banco de dados.
	 * @param newVersion recebe um <code>int</code> com a versão nova do banco de dados.
	 */
	private void recriaBanco(SQLiteDatabase db, int oldVersion, int newVersion){
		// cria tabelas temp e apaga tabelas
		for(String tabela : tabelas){
			if(tabelaExists(db, tabela)){
				criaTabelaTemp(db, tabela);
				apagaTabela(db, tabela);
			}
		}

		// cria uma nova base de dados
		onCreate(db);

		// repopula tabelas
		for(String tabela : tabelas){
			if(tabelaExists(db, tabela + sufixoTabelaTemp)){
				try{
					repopula(db, tabela);
				}
				catch(SQLiteException e){
					Log.d("Excecao ao repopupar banco: ", e.getMessage());
					apagaRegistrosTabela(db, tabela);
					Log.d("Registros removidos: ", String.format("Tabela %s", tabela));
				}
				apagaTabelaTemp(db, tabela);
			}
		}
	}
	
	/** 
	 * Método para excluir os registros de uma tabela do banco de dados.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela que terá seus registros excluídos.
	 */
	private void apagaRegistrosTabela(SQLiteDatabase db, String tabela){
		db.execSQL(String.format("DELETE FROM %s", tabela));
	}

	/** 
	 * Método para excluir uma tabela do banco de dados.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela a ser esxcluída.
	 */
	private void apagaTabela(SQLiteDatabase db, String tabela){
		db.execSQL(String.format("DROP TABLE IF EXISTS %s", tabela));
	}

	/** 
	 * Método para excluir a tabela temporária do banco de dados.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela temporária a ser esxcluída.
	 */
	private void apagaTabelaTemp(SQLiteDatabase db, String tabela){
		apagaTabela(db, tabela + sufixoTabelaTemp);
	}

	/** 
	 * Método para criação da tabela temp, adicionao o sufixo sufixoTabelaTemp ao nome da tabela.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela temporária a ser criada.
	 */
	private void criaTabelaTemp(SQLiteDatabase db, String tabela){
		String tabelaTemp = String.format("%s%s", tabela, sufixoTabelaTemp);
		String queryCriaTabelaTemp = String.format("CREATE TABLE %s AS SELECT * FROM %s", tabelaTemp, tabela);
		db.execSQL(queryCriaTabelaTemp);
	}

	/** 
	 * Método para obter os índices de uma coluna.
	 * @param cursorSelect recebe um cursor (semelhante a um ponteiro) para referenciar uma linha da tabela.
	 * @return um <code>int</code> com os ídices de uma coluna.
	 */
	private int[] getIndicesColunas(Cursor cursorSelect){
		String[] nomeColunas = cursorSelect.getColumnNames();
		int[] indices = new int[nomeColunas.length];
		for(int i = 0; i < indices.length; i++){
			indices[i] = cursorSelect.getColumnIndex(nomeColunas[i]);
		}
		return indices;
	}

	/** 
	 * Método para criar uma <code>String</code> com o comando Insert.
	 * @param indices recebe os índices.
	 * @param tabela recebe o nome da tabela
	 * @param cursorDadosTabelaTemp recebe um cursor (semelhante a um ponteiro) para referenciar uma linha da tabela temporária.
	 * @param cursorTabelaNova recebe um cursor (semelhante a um ponteiro) para referenciar uma linha da tabela a ser criada.
	 * @return uma <code>String</code> com o resultado da inserção dos valores que formaram o comando Insert.
	 */
	private String createInsertString(int[] indices, String tabela, Cursor cursorDadosTabelaTemp, Cursor cursorTabelaNova){
		StringBuilder colunasTemp = new StringBuilder(); // StringBuilder com os nomes das colunas temporárias (col1, col2, col...)
		StringBuilder values = new StringBuilder(); // StringBuilder com valores das colunas (?, ?, ?...)

		for(int indice : indices){
			String coluna = cursorDadosTabelaTemp.getColumnName(indice);

			// Verifica se a coluna de temp existe na da nova estrutura da tabela
			if(cursorTabelaNova.getColumnIndex(coluna) != -1){
				colunasTemp.append(coluna + ",");
				values.append("?,");
			}
		}

		// Apaga a ultima virgula
		colunasTemp.deleteCharAt(colunasTemp.length()-1);
		values.deleteCharAt(values.length()-1);

		String queryInsert = String.format("INSERT INTO %s(%s) values(%s)", tabela, colunasTemp, values);
		return queryInsert;
	}

	/** 
	 * Método para retornar um cursor referente aos índices passados como parâmetro.
	 * @param indices recebe os índices.
	 * @param cursorDadosTabelaTemp recebe um cursor (semelhante a um ponteiro) para referenciar uma linha da tabela temporária.
	 * @param cursorTabelaNova recebe um cursor (semelhante a um ponteiro) para referenciar uma linha da tabela a ser criada.
	 * @return uma <code>String[]</code> com os valores de um cursor.
	 */
	private String[] getValoresCursor(int[] indices, Cursor cursorDadosTabelaTemp, Cursor cursorTabelaNova){
		List<String> valoresTemp = new ArrayList<String>(); // Lista de String com valores temporários (valor1, valor2, valor...)

		for(int indice : indices){
			String valor = cursorDadosTabelaTemp.getString(indice); // obtém todos os valores e retorna-os como String
			String coluna = cursorDadosTabelaTemp.getColumnName(indice);

			// Verifica se a coluna de temp existe na coluna da nova estrutura da tabela
			if(cursorTabelaNova.getColumnIndex(coluna) != -1){
				valoresTemp.add(valor);
			}
		}

		return valoresTemp.toArray(valoresTemp.toArray(new String[valoresTemp.size()]));
	}

	/** 
	 * Método para repopular a tabela usando os dados da tabela temporaria.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela.
	 */
	private void repopula(SQLiteDatabase db, String tabela) throws SQLiteException{
		Cursor cursorDadosTabelaTemp = db.rawQuery(String.format("SELECT * FROM %s%s", tabela, sufixoTabelaTemp), null);
		Cursor cursorTabelaNova = db.rawQuery(String.format("SELECT * FROM %s", tabela), null);

		int[] indices = getIndicesColunas(cursorDadosTabelaTemp);

		// itera pelos valores da tabela e adiciona-os ao List
		if(cursorDadosTabelaTemp.moveToFirst()){

			// cria a string para insert (INSERT INTO tabela(col1, col2) values(?, ?))
			String queryInsert = createInsertString(indices, tabela, cursorDadosTabelaTemp, cursorTabelaNova);

			do{
				// obtém os valores das colunas
				String[] valoresTemp = getValoresCursor(indices, cursorDadosTabelaTemp, cursorTabelaNova);

				// Insere os dados da tabela temp na nova tabela
				db.execSQL(queryInsert, valoresTemp);

			}while(cursorDadosTabelaTemp.moveToNext());
		}
	}

	/** 
	 * Método para verificar se uma tabela existe no banco de dados.
	 * @param db recebe um objeto <code>SQLiteDatabase</code> que refrencia o banco de dados local.
	 * @param tabela recebe o nome da tabela.
	 * @return um <code>boolean</code> referente a consulta:
	 *  - true: a tabela existe no banco de dados
	 *  - false: a tabela não existe no banco de dados.
	 */
	private boolean tabelaExists(SQLiteDatabase db, String tabela){
		boolean tabelaExiste = false;
		try{
			@SuppressWarnings("unused")
			Cursor cursor = db.query(tabela, null, null, null, null, null, null);
			tabelaExiste = true;
		}
		catch (SQLiteException e){
			if (e.getMessage().toString().contains("no such table")){
				tabelaExiste = false;
			}
		}
		return tabelaExiste;
	}
	
	/** Retorna um int com o numero de transacoes maximo permitido
	 * @return um int com o maxRegistrosTransacao
	 */
	public static int getMaxRegistrosTransacao() {
		if(REGISTROS_TRANSACAO == 0){
			Runtime rt = Runtime.getRuntime();
			long maxMemory = rt.maxMemory() / 1024 / 1024;
			if(maxMemory <= 16){
				REGISTROS_TRANSACAO = MAX_REGISTROS_TRANSACAO / 8;
			}
			else if(maxMemory <= 24){
				REGISTROS_TRANSACAO = MAX_REGISTROS_TRANSACAO / 4;
			}
			REGISTROS_TRANSACAO = MAX_REGISTROS_TRANSACAO;
		}
		return REGISTROS_TRANSACAO;
	}

}
