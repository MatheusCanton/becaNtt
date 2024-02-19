# becaNtt
Projeto Beca

# Comandos gits
**GIT ADD**

    git add . adiciona TODAS suas alterações para area "stage" do git. 
    

    git add arquivo.py adiciona  apenas o arquivo.py na area stage.
    

    git add diretorio/ adiciona todas mudanças do diretorio na area stage.

**GIT STATUS**

    git status verifica as mudanças feitas na branch. 
    primeiro git status as mudanças serao mostradas em vermelho , após git add . o comando git status as mudanças serao mostradas em verde.

**GIT COMMIT**

    git commit -m "Mensagem referente as mudanças que estão subindo".
    pega as mudanças que estão na area stage e são colocadas no commit e pronta para realizar o push  

**GIT PULL**

    git pull origin <branch>
    tras as atualizações do seu repositorio com base na origin <branch> 
**GIT PUSH**

    git push origin <branch> 
    leva as atualizações feitas do seu processo para a branch para origin <branch>
**GIT BRANCH**

    git branch
    Mostra em qual branch você está no momento

    git branch <branch>
    Cria uma nova <branch> baseada na <branch> atual, se não tiver com nenhuma branch sera baseada na master

**GIT CHECKOUT**

    git checkout <branch>
    Faz a troca de <branch>

    git checkout -b <branch>
    Faz a troca e a criação de uma nova branch 
