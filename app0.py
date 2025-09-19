from flask import Flask, render_template, request, redirect, url_for, flash
import os

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui'

@app.route('/')
def index():
    return render_template('formulario.html')

@app.route('/enviar', methods=['POST'])
def enviar_formulario():
    # Captura os dados do formulário
    nome_rua = request.form.get('nome_rua')
    numero = request.form.get('numero')
    bairro = request.form.get('bairro')
    situacoes = request.form.getlist('situacao')
    
    # Validação básica
    if not nome_rua or not numero or not bairro or not situacoes:
        flash('Por favor, preencha todos os campos obrigatórios.', 'error')
        return redirect(url_for('index'))
    
    # Aqui você pode processar os dados (salvar no banco, enviar email, etc.)
    print(f"Nome da Rua: {nome_rua}")
    print(f"Número: {numero}")
    print(f"Bairro: {bairro}")
    print(f"Situações: {', '.join(situacoes)}")
    
    flash('Solicitação enviada com sucesso! Em breve entraremos em contato.', 'success')
    return redirect(url_for('index'))

if __name__ == '__main__':
    # Cria a pasta templates se não existir
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    app.run(debug=True)