package dao;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

public class DatabaseHelper {
	private SQLHelper sqlHelper;
	private SQLiteDatabase db;

	private static final String NOME_BANCO = "database";
	private static final int VERSAO = 1;
	private static DatabaseHelper banco;

	private static final String[] DATABASE_TABLES = {
		"Teste"
	};

	private static final String[] DATABASE_TABELAS_REMOVIDAS = {

	};

	private static final String[] DATABASE_CREATE = new String[]{
	"CREATE TABLE IF NOT EXISTS Teste (" +
		"codigo VARCHAR(25), " +
		"nome VARCHAR(50), " +
		"email VARCHAR(20), " +
		"PRIMARY KEY (codigo)" +
	");" +
	""
	};

	private DatabaseHelper(Context context) {
		// cria o SQLHelper
		sqlHelper = new SQLHelper(context, NOME_BANCO, VERSAO, DATABASE_TABLES, DATABASE_CREATE, DATABASE_TABELAS_REMOVIDAS);

		//abre o banco de dados para escrita e leitura
		db = sqlHelper.getWritableDatabase();
	}

	public synchronized static DatabaseHelper getInstance(Context context){
		if(banco == null || !banco.db.isOpen()){
			banco = new DatabaseHelper(context);
		}
		return banco;
	}
	
	public synchronized boolean isOpen(){
		if(db != null && db.isOpen()){
			return true;
		}
		return false;
	}

	public synchronized void fechar(){
		if(sqlHelper != null){
			sqlHelper.close();
		}
		if(db != null && db.isOpen()){
			db.close();
		}
	}

	public synchronized SQLiteDatabase getDb() {
		return db;
	}

}
