<h1>Projeto MVP da Squad Finaceira [Darth Vader]</h1>
<br>
<p><b>Product Owner:</b> Fernanda Faria - Gerente Financeiro</p>
<p><b>Dev Team:</b> Everis</p>
<p><b>Metodologia:</b> Git Flow</p>
<p><b>URL do projeto GIT:</b> http://ec2-54-89-13-240.compute-1.amazonaws.com/BigData-Financeiro/mvp.git</p>
<br>
Neste repositório iremos adicionar codificação referente ao Motor de Ingestão Big Data desenvolvido pela Squad Financeira representada pela Everis.
<br>
<h2>Características do Motor de ingestão</h2>
<ul>
	<li>Engine desenvolvido em Python 2.7</li>
	<li>Capturar dados históricos e correntes de bases Oracle via Jobs SQOOP</li>
	<li>Capturar dados históricos e correntes de bases ADABAS via captura e processamento de arquivos com SPARK</li>
	<li>Capturar dados históricos e correntes de bases Teradata</li>
</ul>
<br>
<h2>Instruções para manuseio do Git:</h2>

<h3>Primeiro acesso?</h3>
<ul>
	<li>Baixe a versão mais recente do client Git (disponível para Windows, Linux e MacOS): https://git-scm.com/downloads</li>
	<li>Abra o Git Bash</li>
	<li>Crie um diretório reservado e em seguida clone o projeto via Git Bash através do comando git clone:
		<ul><li>exemplo: $ git clone "http://ec2-54-89-13-240.compute-1.amazonaws.com/BigData-Financeiro/mvp.git"</li></ul></li>
	<li>A url do projeto encontra-se na parte superior desta mesma página</li>
	<li>Por padrão, o git sempre baixará a Branch Master</li>
	<li>Configure seu perfil (nome de usuário e email), exemplo:<ul>
		<li><b>$ git config --global user.name “Seu Nome”</b></li>
		<li><b>$ git config --global user.email “seu_email@everis.com”</b></li></ul></li>
	<li>Somente após configurar seu perfil você será capaz de atuar no git, queremos saber quem é você! ;)</li>
</ul>
<br>
<h3>Configurando ambiente local de trabalho</h3>
<p>Após realizar um git clone, inicialize o git lab flow</p>
<ul><li><b>$ git flow init</b></li></ul>
<p>O git irá solicitar todas as branchs da natureza GIT FLOW para configurar localmente:</p>
<ul>
    <li><b>$ Which branch should be used for bringing forth production releases?</b>
        <ul><li><b>$ Branch name for production releases: [master] --> Aperte a tecla ENTER<b></li></ul>
    </li>
    <li><b>$ Which branch should be used for integration of the "next release"?</b>
        <ul><li><b>$ Branch name for "next release" development: [develop] --> Aperte a tecla ENTER<b></li></ul>
    </li>
    <li><b>$ How to name your supporting branch prefixes?</b>
        <ul><li><b>$ Feature branches? [feature/] --> Aperte a tecla ENTER<b></li></ul>
    </li>
    <li><b>$ Bugfix branches? [bugfix/] --> Aperte a tecla ENTER</b></li>
    <li><b>$ Release branches? [release/] --> Aperte a tecla ENTER</b></li>
    <li><b>$ Hotfix branches? [hotfix/] --> Aperte a tecla ENTER</b></li>
    <li><b>$ Support branches? [support/] --> Aperte a tecla ENTER</b></li>
    <li><b>$ Version tag prefix? [tag/] --> Aperte a tecla ENTER</b></li>
    <li><b>$ Hooks and filters directory? [C:/Users/......./git/mvp/.git/hooks] --> Aperte a tecla ENTER</b></li>
</ul>
<br>
<h3>Git Flow</h3>
<p>O Git Flow é um modelo de se trabalhar onde você existem 3 Branch principais: Master, Develop e Release. Através do git flow o desenvolvedor cria automaticamente branch's paralelas e temporárias para cada funcionalidade nova (feature). Estas branch's de funcionalidade ficam com o prefixo "feature...", quando o desenvolvedor termina sua funcionalidade ele encerra a feature e automaticamente ela é movida para a branch develop. </p>
<br>
<p>Para iniciar uma nova feature:</p>
<ul><li><b>$ git flow feature start Nome_Da_Sua_Funcionalidade</b></li></ul>
<p>Ao executar o comando acima o Git avisa o desenvolvedor que uma branch temporária é criada baseada na branch "Develop"</p>
<p>O desenvolvedor deve realizar todas as suas criações e alterações nesta etapa e após finalizar deve-se commitar: </p>
<ul>
    <li><b>$ git status</b></li>
    <li><b>$ git add .</b></li>
    <li><b>$ git commit -m "Comentario detalhado sobre o que foi realizado."</b></li>
    <ul><li>Lembre-se, para ajudar a equipe coloque sempre comentários claros e objetivos. Nada de usar palavras curtas ou que não sejam de fácil compreensão para todos os membros do DEV TEAM. Isso ajuda a rastreabilidade da informação comitada.</li></ul>
</ul>
<p>Para encerrar a sua feature:</p>
<ul><li><b>$ git flow feature finish Nome_Da_Sua_Funcionalidade</b></li></ul>
<p>Ao finalizar o seu desenvolvimento realize um PUSH</p>
<ul><li><b>$ git push</b></li></ul>
<br>
<h3>Publicando Releases</h3>
<p>Para iniciar o processo de publicação você deve estar na branch Develop e em seguida iniciar uma release da seguinte maneira:</p>
<ul>
    <li><b>$ git flow release start Nome_Da_Release X.X</b></li>
    <ul><li><b>Onde X.X se refere respectivamente a ANO.VERSÃO, exemplo: 2018.001</b></li></ul>
</ul>
<p>Após iniciar uma release será necessário abrir um Merge Request.</p>
<ul>
    <li>Acesse o projeto no git web (Ex.: www.....)</li>
    <li>No menu lateral esquerdo, procure a opção Merge Request, clique nela</li>
    <li>No canto superior direito, clique no botão "New Merge Request"</li>
    <li>Compare a branch Master com sua Branch de Release</li>
    <li>Clique no botão "Compare Branchs and Continue"</li>
    <li>Escreva um título e uma descrição coerentes, sem utilizar html, apenas texto.</li>
    <li>No canto inferior da tela clique no botão "Submit Merge Request"</li>
    <li>Aguarde o admin aprovar sua solicitação</li>
</ul>
<p>Quando o Merge Request estiver aprovado, você já poderá finalizar o seu Release:</p>
<ul>
    <li><b>$ git flow release finish Nome_Da_Release X.X</b></li>
</ul>
<br>
<h3>Correção de Bug's de Produção</h3>
<p>Existem momentos não previstos em que muitas vezes um bug pode aparecer por N motivos, mudança de tecnologia, ambiente produtivo imprevisível, mudanças de API conflitosas, etc. Pode ser necessário realizar uma correção em seu código, como começar uma correção? A partir da Branch MASTER, faça:</p>
<ul>
    <li><b>$ git flow hotfix start X.X.X</b></li>
</ul>
<p>Caso mais de um desenvolvedor trabalhe na correção do hotfix, é possível publicar:</p>
<ul>
    <li><b>$ git flow hotfix publish X.X.X</b></li>
</ul>
<p>Ao finalizar a correção, encerre seu bugfix:</p>
<ul>
    <li><b>$ git flow hotfix finish X.X.X</b></li>
</ul>
<p><b>Onde X.X.X se refere respectivamente a ANO.VERSÃO_PRODUÇÃO.VERSÃO_BUGFIX. Se em produção existe um Release 2018.001 então ficará exemplo: 2018.001.01</b></p>
<br>
<h3>Funcionalidades</h3>
<ul>
    <li><b>$ git flow init</b>: Pré-requisito para inicializar o projeto</li>
    <li><b>$ git pull</b>: Baixar artefatos do git</li>
    <li><b>$ git push</b>: Enviar artefatos para o git</li>
    <li><b>$ git add .</b>: Adicionar artefatos para serem comitados em seguida</li>
    <li><b>$ git commit -m "Comentário detalhado"</b>: Realização do commit</li>
    <li><b>$ git gui</b>: Git Visual nativo (é possível realizar git add, commit, pull e push visualmente)</li>
    <li><b>$ git status</b>: Exibe artefatos novos que precisam ser commitados, se houver</li>
    <li><b>$ git branch [-a|-l]</b>: Exibe lista de branch locais. Parâmetro -a exibe locais e remotas</li>
    <li><b>$ git log</b>: Exibe todos o commits da branch atual</li>
    <li><b>$ git diff A B</b>: compara as diferenças entre commit's. Necessário pegar o ID dos commit's que deseja comparar (A e B) no git log</li>
    <li><b>$ git reset --hard</b>: Volta ao estado inicial antes do seu commit</li>
    <li><b>$ git merge BRANCH_DESTINO_MERGE</b>: Realiza o merge entre sua branch atual e a branch destino que deseja</li>
    <li><b>$ git flow feature [start|publish|finish|pull|push] [origin] nome_da_feature</b></li>
    <li><b>$ git flow release [start|publish|finish] [origin] nome_da_feature</b></li>
</ul>
<p><b>Feature:</b> É uma funcionalidade nova ou atual no sistema, essa funcionalidade pode ser desenvolvida por mais de um programador ao mesmo tempo, o primeiro programador deverá criar a <b>feature</b> e em seguida realizar um <b>publish</b> para os demais programadores obterem as funcionalidades que já estão prontas para continuar o desenvolvimento. Quando todos os desenvolvedores terminarem os "pedaços" da <b>feature</b>, lembrem-se de realizarem sempre um <b>commit</b> e em seguida um <b>push</b>, por fim, faça um <b>finish</b> para encerrar sua branch feature local. Caso você seja o segundo desenvolvedor a realizar o commit e esteja recebendo qualquer tipo de mensagem de erro, certifique-se de realizar um git pull, git status, commit, caso não tenha nada de novo ao realizar o commit faça um <b>git push</b> e em seguida um <b>finish</b>. Quando todos os desenvolvedores terminarem a feature uma release nova se criada pelo Admninistrador.</p>
<br>
<h3>Branch's</h3>
<ul>
    <li><b>master</b>: Está é a branch principal, a mais estável e a ultima versão disponível em produção.</li>
    <li><b>develop</b>: Está branch possuí todas as funcionalidades de produção e as novas, todos os desenvolvedores trabalham apenas com base nesta branch.</li><br>
    <li><b>feature/descricao</b>: [BRANCH TEMPORÁRIA] Está branch não existe de fato, ela é criada automaticamente pelo git flow [Start], compartilhada entre os desenvolvedores [Publish], e apagada + mergiada com a branch Develop automaticamente [Finish]</li>
    <li><b>release/descricao</b>: [BRANCH TEMPORÁRIA] Está branch não existe de fato, ela é criada automaticamente pelo git flow [Start], compartilhada entre os desenvolvedores [Publish], e apagada + mergiada com a branch Develop automaticamente [Finish]</li>
    <li><b>hotfix/descricao</b>: [BRANCH TEMPORÁRIA] Está branch não existe de fato, ela é criada automaticamente pelo git flow [Start], compartilhada entre os desenvolvedores [Publish], e apagada + mergiada com a branch Develop automaticamente [Finish]</li>
</ul>
<br>
<h2>Caso tenha dúvidas, não deixe de procurar o administrador: marco.antonio.pereira@everis.com</h2>






