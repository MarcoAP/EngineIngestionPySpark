from Logger import Logger
from LoggerEnum import LoggerEnum
from datetime import date, timedelta
from pyspark.sql.functions import max as max_
from pyspark import SparkContext, HiveContext
from PropertiesHandler import PropertiesHandler
import sys
import subprocess
import datetime


class Main:

    def __init__(self, partition_date):
        self.logger = Logger()
        self.properties = PropertiesHandler('RefinedConfig.properties')
        self.props_section = str(sys.argv[2])
        self.date_fmt = "%Y-%m-%d"
        self.partition_name = self.properties.getProperties(self.props_section, "partition_name")
        self.partition_date = partition_date
        database = self.properties.getProperties(self.props_section, "database")
        table = self.properties.getProperties(self.props_section, "table")
        self.db_tb = database+"."+table
        self.sc = SparkContext.getOrCreate()
        self.hive = HiveContext(self.sc)
        self.hive.setConf("hive.exec.dynamic.partition", "true")
        self.hive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    def run_hive_query(self, query):
        try:
            cmd = "hive -e \"{0}\"".format(query)
            self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to run query {0}".format(query))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.ERROR, "Successfully ran query {0}".format(query))
            return True

    def drop_hive_table(self, table, purge=True):
        try:
            cmd = "hive -e \"DROP TABLE {0} {1}\"".format(table, "PURGE" if purge else "")
            self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to drop table {0}".format(table))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.ERROR, "Successfully dropped table {0}".format(table))
            return True

    def create_df(self, query):
        try:
            self.logger.message(LoggerEnum.INFO, "Creating dataframe from input query: {0}..."
                                .format(query.replace("\n", " ")[0:100]))
            df = self.hive.sql(query)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to create dataframe from query: {0}..."
                                .format(query.replace("\n", " ")[0:100]))
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Created dataframe successfully from query: {0}..."
                                .format(query.replace("\n", " ")[0:100]))
            return df

    def union_dfs(self, df1, df2):
        try:
            self.logger.message(LoggerEnum.INFO, "Uniting two dataframes")
            union_df = df1.unionAll(df2)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to union both dataframes. Aborting application")
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "United both dataframes successfully")
            return union_df

    def join_dfs(self, df1, df2, join_type, cols):
        try:
            self.logger.message(LoggerEnum.INFO, "Joining two dataframe with method: {0} on columns: {1}"
                                .format(join_type, ", ".join(cols)))
            join_df = df1.join(df2, cols, join_type)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to join both dataframes. Aborting application")
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Joined both dataframes successfully")
            return join_df

    def filter_df_on_max(self, df, col_name):
        try:
            self.logger.message(LoggerEnum.INFO, "Beginning filtering operation on dataframe using max from column {0}"
                                .format(col_name))
            aux = df.groupBy().agg(max_(col_name)).collect()[0][0]
            new_df = df.filter(df[col_name] == aux)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to filter column {0} from dataframe. Aborting application"
                                .format(col_name))
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Filtering column {0} on dataframe was successful".format(col_name))
            return new_df

    def filter_df_on_value(self, df, col_name, col_value):
        try:
            self.logger.message(LoggerEnum.INFO, "Beginning filtering operation on dataframe where {0} = {1}"
                                .format(col_name, col_value))
            new_df = df.filter(df[col_name] == col_value)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to filter {0} = {1} from dataframe. Aborting application"
                                .format(col_name, col_value))
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Filtering {0} = {1} on dataframe was successful".format(col_name,
                                                                                                          col_value))
            return new_df

    def hive_drop_one_partition(self):
        self.logger.message(LoggerEnum.INFO, "Beginning drop partition {0} from {1}"
                            .format(self.partition_date, self.db_tb))
        try:
            if datetime.datetime.strptime(self.partition_date, self.date_fmt):
                cmd = "hive -e \"ALTER TABLE {0} DROP PARTITION({1}='{2}')\""\
                                                                    .format(self.db_tb, self.partition_name,
                                                                            self.partition_date)

                self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Drop partition {0} from table {1} failed"
                                .format(self.partition_date, self.db_tb))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            return False
        else:
            self.logger.message(LoggerEnum.INFO, "Dropped {0} from {1}".format(self.partition_date, self.db_tb))
            return True

    def hive_drop_many_partitions(self, last_date):
        self.logger.message(LoggerEnum.INFO, "Beginning drop partitions >= {0} from {1}"
                            .format(last_date, self.db_tb))
        try:
            if datetime.datetime.strptime(last_date, self.date_fmt):
                cmd = "hive -e \"ALTER TABLE {0} DROP PARTITION({1}>='{2}')\""\
                                                                    .format(self.db_tb, self.partition_name, last_date)

                self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Drop partition >= {0} from table {1} failed"
                                .format(last_date, self.db_tb))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            return False
        else:
            self.logger.message(LoggerEnum.INFO, "Dropped {0} from {1}".format(last_date, self.db_tb))
            return True

    def hive_drop_partitions_5anos(self, last_date):
        self.logger.message(LoggerEnum.INFO, "Beginning drop partitions <= {0} from {1}"
                            .format(last_date, self.db_tb))
        try:
            if datetime.datetime.strptime(last_date, self.date_fmt):
                cmd = "hive -e \"ALTER TABLE {0} DROP PARTITION({1}<='{2}')\""\
                                                                    .format(self.db_tb, self.partition_name, last_date)

                self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
                subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Drop partition <= {0} from table {1} failed"
                                .format(last_date, self.db_tb))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            return False
        else:
            self.logger.message(LoggerEnum.INFO, "Dropped {0} from {1}".format(last_date, self.db_tb))
            return True

    def _refined_truncate_table(self):
        self.logger.message(LoggerEnum.INFO, "Beginning truncate table {0} ".format(self.db_tb))
        try:
            cmd = "hive -e \"TRUNCATE TABLE {0}\"".format(self.db_tb)
            self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(cmd))
            subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.logger.message(LoggerEnum.ERROR, "Truncate table {0} failed. Exiting application".format(self.db_tb))
            self.logger.message(LoggerEnum.ERROR, str(e.output))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Truncated table {0} successfully".format(self.db_tb))
            return True

    def default_refined_ingestion(self, df):
        self.logger.message(LoggerEnum.INFO, "Beginning {0} ingestion".format(self.db_tb))
        try:
            if len(df.take(1)) == 0:
                self.logger.message(LoggerEnum.WARN, "Dataframe {0} is empty. Aborting insert".format(self.db_tb))
                return False
            else:
                self.logger.message(LoggerEnum.INFO, "Dataframe {0} created successfully. Beginning insert operation"
                                    .format(self.db_tb))

            df.write.format("parquet").partitionBy(self.partition_name).mode("append").insertInto(self.db_tb)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Insert on table {0} failed. Exiting application"
                                .format(self.db_tb))
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Loaded {0} successfully"
                                .format(self.db_tb))

    def refined_ingestion_no_partition(self, df):
        self.logger.message(LoggerEnum.INFO, "Beginning {0} ingestion".format(self.db_tb))
        try:
            if len(df.take(1)) == 0:
                self.logger.message(LoggerEnum.WARN, "Dataframe {0} is empty. Aborting insert".format(self.db_tb))
                return False
            else:
                self.logger.message(LoggerEnum.INFO, "Dataframe {0} created successfully. Beginning insert operation"
                                    .format(self.db_tb))

            df.write.format("parquet").mode("append").insertInto(self.db_tb)
        except Exception as e:
            self.logger.message(LoggerEnum.ERROR, "Failed to insert on table {0}. Exiting application"
                                .format(self.db_tb, self.partition_date))
            self.logger.message(LoggerEnum.ERROR, str(e))
            exit(-1)
        else:
            self.logger.message(LoggerEnum.INFO, "Loaded {0} successfully on partition: {1}"
                                .format(self.db_tb, self.partition_date))

    def refined_reproc(self, col_name, col_value, partition_option):
        self.logger.message(LoggerEnum.INFO, "Beginning reprocess. Table: {0}, column name: {1}, column value: {2}, "
                            "partitioned table: {3}".format(self.db_tb, str(col_name), str(col_value),
                                                            str(partition_option)))

        tmp_table = "default.tmp_TC014553_spark_main"
        df_ini = self.create_df("SELECT * FROM {0} LIMIT 1".format(self.db_tb))
        hive_schema = df_ini.dtypes
        columns = ",".join([col[0] for col in hive_schema])

        query = "CREATE TABLE {0} AS SELECT {1} FROM {2} WHERE {3} <> '{4}'"\
                .format(tmp_table, columns, self.db_tb, col_name, col_value)
        self.run_hive_query(query)

        filtered_df = self.create_df("SELECT * FROM {0}".format(tmp_table))
        self._refined_truncate_table()

        if partition_option:
            self.default_refined_ingestion(filtered_df)
        else:
            self.refined_ingestion_no_partition(filtered_df)

        self.partition_date = str(col_value)

        # Incluir novas tabelas aqui
        if self.props_section == "senha_utilizada":
            self.senha_utilizada_ingestion()

        self.drop_hive_table(tmp_table)

    def hdfs_rm(self, path, force=True, skip_trash=True):
        if isinstance(path, list):
            for ipath in path:
                try:
                    command = "hdfs dfs -rm {0} {1} {2}".format("-r" if bool(force) else "",
                                                                "-skipTrash" if bool(skip_trash) else "", str(ipath))
                    self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(command))
                    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
                except subprocess.CalledProcessError as e:
                    self.logger.message(LoggerEnum.ERROR, str(e.returncode) + " " + str(e.output))
                    return -1
                else:
                    self.logger.message(LoggerEnum.INFO, "Done removing {0}".format(ipath))
            return 0
        elif isinstance(path, basestring):
            try:
                command = "hdfs dfs -rm {0} {1} {2}".format("-r" if bool(force) else "",
                                                            "-skipTrash" if bool(skip_trash) else "", str(path))
                self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(command))
                subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
            except subprocess.CalledProcessError as e:
                self.logger.message(LoggerEnum.ERROR, str(e.returncode) + " " + str(e.output))
                return -1
            else:
                self.logger.message(LoggerEnum.INFO, "Done removing {0}".format(path))
            return 0
        else:
            self.logger.message(LoggerEnum.WARN, "Could not remove {0} because it's not a string nor a list. Type:"
                                .format(path, type(path)))
            return -1

    def hdfs_create_dir(self, path, force=True):
        if isinstance(path, list):
            for ipath in path:
                try:
                    command = "hdfs dfs -mkdir {0} {1}".format("-p" if bool(force) else "", str(ipath))
                    self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(command))
                    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
                except subprocess.CalledProcessError as e:
                    self.logger.message(LoggerEnum.ERROR, str(e.returncode) + " " + str(e.output))
                    return -1
                else:
                    self.logger.message(LoggerEnum.INFO, "Done creating {0}".format(ipath))
            return 0
        elif isinstance(path, basestring):
            try:
                command = "hdfs dfs -mkdir {0} {1}".format("-p" if bool(force) else "", str(path))
                self.logger.message(LoggerEnum.INFO, "Running shell command: {0}".format(command))
                subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
            except subprocess.CalledProcessError as e:
                self.logger.message(LoggerEnum.ERROR, str(e.returncode) + " " + str(e.output))
                return -1
            else:
                self.logger.message(LoggerEnum.INFO, "Done creating {0}".format(path))
            return 0
        else:
            self.logger.message(LoggerEnum.WARN, "Could not create {0} because it's not a string nor a list. Type: {1}"
                                .format(path, type(path)))
            return -1

    def senha_utilizada_ingestion(self):
        self.logger.message(LoggerEnum.INFO, "Beginning {0} ingestion on partition: {1}".format(self.db_tb,
                                                                                                self.partition_date))

        hdfs_tmp_dir = "/tmp/squadfi_spark/"
        if self.hdfs_create_dir(hdfs_tmp_dir) != 0:
            exit(-1)
        self.sc.setCheckpointDir("hdfs://"+hdfs_tmp_dir)

        # Criacao do Dataframe do AC34. Realiza esses passos para que exista apenas uma senha por chave
        # Cria dataframe do AC34 com campos:
        # MAX(dat_envio_email_senha), cod_loja, cod_plu, cod_senha, dt_processamento
        # WHERE cod_senha IS NOT NULL, ja que no arquivo podem vir alteracoes de preco que nao sao de senha
        df_ac34_2 = self.create_df(self.properties.getProperties(self.props_section, "q_ac34_2"))
        # Filtra o dataframe anterior com base no MAX(dt_processamento)
        new_ac34_2 = self.filter_df_on_max(df_ac34_2, self.partition_name)
        # Cria dataframe do AC34 com campos:
        # dat_envio_email_senha, cod_usu_cad_senha, nom_usu_cad_senha, cod_loja, cod_plu, cod_senha
        df_ac34_1 = self.create_df(self.properties.getProperties(self.props_section, "q_ac34_1"))
        # Une os dataframes anteriores
        join_ac34 = self.join_dfs(df_ac34_1, new_ac34_2, "inner",
                                  ["dat_envio_email_senha", "cod_loja", "cod_plu", "cod_senha"])

        # Cria dataframe da estrutura mercadologica
        df_merc = self.create_df(self.properties.getProperties(self.props_section, "q_merc"))
        # Filtra o dataframe anterior com base no MAX(dt_processamento)
        new_merc = self.filter_df_on_max(df_merc, self.partition_name)

        # Cria dataframe da estrutrua operacional financeira
        df_oper = self.create_df(self.properties.getProperties(self.props_section, "q_oper"))
        # Filtra o dataframe anterior com base no MAX(dt_processamento)
        new_oper = self.filter_df_on_max(df_oper, self.partition_name)

        # Cria dataframe que une MR3 (RD), MX2 (senha) e SIAC (cupom), trazendo apenas uma senha por chave (loja,
        # dia/hora, plu)
        # Query, da mais interna ate a final (observar alias das tabelas na query):
        # SIAC: Une MR3 (RD) ao cupom do SIAC, para obter informacoes de: qtd de produtos vendidos, valor da venda,
        # valor da RD e data/hora de inicio da venda
        # T2: Une o resultado do SIAC com parte do MX2, aplicando as regras de horario da senha. E para gerar apenas um
        # registro por chave, faz o MAX da data/hora de aplicacao da senha
        # T1: Une o resultado da t2 com o MX2, para que seja possivel obter os demais campos do MX2
        df_mx2_siac = self.create_df(self.properties.getProperties(self.props_section, "q_mx2_siac")
                                     .format(self.partition_date))

        tmp_ac34 = "tmp_ac34"
        tmp_merc = "tmp_merc"
        tmp_oper = "tmp_oper"
        tmp_mx2_siac = "tmp_mx2_siac"
        tmp_mx2_siac_2 = "tmp_mx2_siac_2"

        join_ac34.registerTempTable(tmp_ac34)
        new_merc.registerTempTable(tmp_merc)
        new_oper.registerTempTable(tmp_oper)
        df_mx2_siac.registerTempTable(tmp_mx2_siac)

        # Cria dataframe final, juntando os demais criados acima
        final_df_1 = self.create_df(self.properties.getProperties(self.props_section, "q_final")
                                    .format(self.partition_date, tmp_mx2_siac, tmp_oper, tmp_merc, tmp_ac34))
        print final_df_1.take(1)
        self.refined_ingestion_no_partition(final_df_1)

        # Cria dataframe igual ao df_mx2_siac, mas com as RDs que ficaram sem uma senha associada, mesmo apos a ingestao
        # anterior
        # Unica mudanca da logica esta na tabela SIAC (observar alias das tabelas), na qual sao consideradas apenas RDs
        # que nao foram contempladas na tabela rfzd_financeiro.senha_utilizada
        # Alem disso, o horario de geracao da senha e desconsiderado ao juntar o MX2 com o SIAC. E considerada a ultima
        # senha que corresponde com a chave: loja dia plu
        df_mx2_siac_2 = self.create_df(self.properties.getProperties(self.props_section, "q_mx2_siac_2")
                                       .format(self.partition_date))

        df_mx2_siac_2.registerTempTable(tmp_mx2_siac_2)

        # Cria dataframe final, juntando todos os demais criados acima, mas considerando apenas as RDs que o processo
        # normal nao associou a uma senha
        final_df_2 = self.create_df(self.properties.getProperties(self.props_section, "q_final_2")
                                    .format(self.partition_date, tmp_mx2_siac_2, tmp_oper, tmp_merc, tmp_ac34))
        print final_df_2.take(1)
        # Faz checkpoint do RDD, pois nao e possivel ter um dataframe que le e escreve na mesma tabela
        final_df_2.rdd.checkpoint()
        new_df = self.hive.createDataFrame(final_df_2.rdd, final_df_2.schema)
        print new_df.take(1)
        self.refined_ingestion_no_partition(new_df)
        self.hdfs_rm(hdfs_tmp_dir)
        self.sc.stop()

    def apur_margem_venda_ingestion(self, dt_beginning):

        drop_partitions = self.hive_drop_many_partitions(dt_beginning)
        if drop_partitions:
            self.logger.message(LoggerEnum.INFO, "Beginning {0} ingestion on partition: {1}".format(self.db_tb,
                                                                                                    self.partition_date))

            df_bonus_cel = self.create_df(self.properties.getProperties(self.props_section, "q_bonus_cel").format(
                dt_beginning, self.partition_date))

            df_fidelidade = self.create_df(self.properties.getProperties(self.props_section, "q_fidelidade").format(
                dt_beginning, self.partition_date))

            tmp_bonus_cel = "tmp_bonus_cel"
            tmp_fidelidade = "tmp_fidelidade"
            df_bonus_cel.registerTempTable(tmp_bonus_cel)
            df_fidelidade.registerTempTable(tmp_fidelidade)

            df_venda = self.create_df(self.properties.getProperties(self.props_section, "q_venda").format(
                dt_beginning, self.partition_date, tmp_fidelidade, tmp_bonus_cel))

            #df_merc = self.create_df(self.properties.getProperties(self.props_section, "q_merc"))
            #new_merc = self.filter_df_on_max(df_merc, "dt_processamento")

            #df_oper = self.create_df(self.properties.getProperties(self.props_section, "q_oper"))
            #new_oper = self.filter_df_on_max(df_oper, "dt_processamento")

            df_ge01 = self.create_df(self.properties.getProperties(self.props_section, "q_ge01").format(dt_beginning,
                                                                                                        self.
                                                                                                        partition_date))


            tmp_venda = "tmp_venda"
            #tmp_merc = "tmp_merc"
            #tmp_oper = "tmp_oper"
            tmp_ge01 = "tmp_ge01"

            #new_merc.registerTempTable(tmp_merc)
            #new_oper.registerTempTable(tmp_oper)
            df_ge01.registerTempTable(tmp_ge01)
            df_venda.registerTempTable(tmp_venda)

            final_df = self.create_df(self.properties.getProperties(self.props_section, "q_final")
                                      .format(tmp_venda, tmp_ge01))

            self.default_refined_ingestion(final_df)
            self.sc.stop()
        else:
            exit(-1)

    def apur_margem_venda_ingestion_full(self, dt_beginning):

            self.logger.message(LoggerEnum.INFO, "Beginning {0} ingestion on partition: {1}".format(self.db_tb,
                                                                                                    self.partition_date))

            df_bonus_cel = self.create_df(self.properties.getProperties(self.props_section, "q_bonus_cel").format(
                dt_beginning, self.partition_date))

            df_fidelidade = self.create_df(self.properties.getProperties(self.props_section, "q_fidelidade").format(
                dt_beginning, self.partition_date))

            tmp_bonus_cel = "tmp_bonus_cel"
            tmp_fidelidade = "tmp_fidelidade"
            df_bonus_cel.registerTempTable(tmp_bonus_cel)
            df_fidelidade.registerTempTable(tmp_fidelidade)

            df_venda = self.create_df(self.properties.getProperties(self.props_section, "q_venda").format(
                dt_beginning, self.partition_date, tmp_fidelidade, tmp_bonus_cel))

            #df_merc = self.create_df(self.properties.getProperties(self.props_section, "q_merc"))
            #new_merc = self.filter_df_on_max(df_merc, "dt_processamento")

            #df_oper = self.create_df(self.properties.getProperties(self.props_section, "q_oper"))
            #new_oper = self.filter_df_on_max(df_oper, "dt_processamento")

            df_ge01 = self.create_df(self.properties.getProperties(self.props_section, "q_ge01").format(dt_beginning,
                                                                                                        self.
                                                                                                        partition_date))


            tmp_venda = "tmp_venda"
            #tmp_merc = "tmp_merc"
            #tmp_oper = "tmp_oper"
            tmp_ge01 = "tmp_ge01"

            #new_merc.registerTempTable(tmp_merc)
            #new_oper.registerTempTable(tmp_oper)
            df_ge01.registerTempTable(tmp_ge01)
            df_venda.registerTempTable(tmp_venda)

            final_df = self.create_df(self.properties.getProperties(self.props_section, "q_final")
                                      .format(tmp_venda, tmp_ge01))

            self.default_refined_ingestion(final_df)
            self.sc.stop()



    def create_kmeans_cluster(self):
        from pyspark.ml.clustering import KMeans, KMeansModel
        from numpy import array
        from math import sqrt
        ini_q = "SELECT datamovto, codloja, numterminal, numseqoperacao, codinternoproduto FROM siac_store_hist.cuponsitens WHERE " \
                "codtipooperacao = 0 AND codloja = 1302 GROUP BY datamovto, codloja, numterminal, numseqoperacao, codinternoproduto"
        df = self.create_df(ini_q)


if __name__ == "__main__":
    exec_options = ["senha_utilizada", "senha_nao_utilizada", "apur_margem_venda"]
    if len(sys.argv) < 3:
        print "Need at least 3 arguments"
        exit(-1)
    if any(sys.argv[2] in option for option in exec_options):
        if sys.argv[1] == "-insert":
            if sys.argv[2] == "senha_utilizada":
                Main(sys.argv[3]).senha_utilizada_ingestion()
            if sys.argv[2] == "apur_margem_venda":
                Main(sys.argv[3]).apur_margem_venda_ingestion(str(sys.argv[4]))
            if sys.argv[2] == "apur_margem_venda_full":
                Main(sys.argv[3]).apur_margem_venda_ingestion_full(str(sys.argv[4]))
        elif sys.argv[1] == "-drop":
            Main(sys.argv[3]).hive_drop_one_partition()
        elif sys.argv[1] == "-reproc":
            Main(sys.argv[4]).refined_reproc(str(sys.argv[3]), str(sys.argv[4]), bool(int(sys.argv[5])))
        elif sys.argv[1] == "-expurgo":
            Main(sys.argv[3]).hive_drop_partitions_5anos(str(sys.argv[3]))
    else:
        print "Option: {0} doesn't exists in available options: {1}. Aborting...".format(sys.argv[2],
                                                                                         ", ".join(exec_options))
        exit(-1)
