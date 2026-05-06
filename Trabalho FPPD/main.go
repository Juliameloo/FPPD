//Grupo: Júlia Melo e Edurdo Dieter (2026)
//Código para o trabalho de sistemas distribuidos (eleicao em anel)
//https://docs.google.com/document/d/1D3wlxmbGS4vFgu1ISp0O1EsIzpJPPTbxCvT9rNPMnrk/edit?usp=sharing

package main

import (
	"fmt"
	"sync"
	"time"
)

type mensagem struct {
	tipo  int    // Tipo da mensagem para fazer o controle do que fazer
	corpo [6]int // Conteudo da mensagem para colocar os ids
}

var (
	chans = []chan mensagem{ // vetor de canias para formar o anel de eleicao
		make(chan mensagem, 10), //Canal com buffer (com tamanho 10)
		make(chan mensagem, 10),
		make(chan mensagem, 10),
		make(chan mensagem, 10),
	}
	controle = make(chan int) //Canal controlador (linha direta entre o Ring e o canal)
	wg       sync.WaitGroup
)

func ElectionControler(in chan int) {
	defer wg.Done()
	var temp mensagem

	// TESTE 1: Falhar Processo 0
	temp.tipo = 2
	chans[3] <- temp
	fmt.Printf("Controle: mudar o processo 0 para falho\n")
	fmt.Printf("Controle: confirmação do processo %d\n", <-in) //Esperar e imprimir confirmação

	// Processo 1 inicia a eleição
	temp.tipo = 1
	temp.corpo[0] = 1 // Candidato atual
	temp.corpo[1] = 1 // Quem iniciou a eleição
	chans[0] <- temp
	fmt.Println("Controle: iniciar eleição pelo processo 1")

	time.Sleep(time.Second) //esperar eleição acabar (Líder sera o 3)

	// TESTE 2: Falhar Processo 3
	temp.tipo = 2
	chans[2] <- temp
	fmt.Printf("\nControle: mudar o processo 3 para falho\n")
	fmt.Printf("Controle: confirmação do processo %d\n", <-in)

	// Processo 2 inicia eleição
	temp.tipo = 1
	temp.corpo[0] = 1
	temp.corpo[1] = 1
	chans[0] <- temp
	fmt.Println("Controle: iniciar eleição pelo processo 2")

	time.Sleep(time.Second) //Líder sera 2

	// ENCERRAR Eleição
	temp.tipo = 4
	for i := 0; i < 4; i++ {
		chans[i] <- temp
	}
	fmt.Println("\n Processo controlador concluído")
}

func ElectionStage(TaskId int, in chan mensagem, out chan mensagem, leader int) {
	defer wg.Done()

	var actualLeader int = leader
	var bFailed bool = false
	var jaInicieiEleicao bool = false // Para saber se a mensagem deu a volta

	for {
		temp := <-in // O processo fica em espera  até receber algo

		if bFailed && temp.tipo != 4 { // Se falhar apenas passe para o próximo (ENCERRAR loop)
			out <- temp // Retransmite a mensagem para o próximo processo no anel
			continue    // Volta para o início do loop (sem executar o resto)
		}

		switch temp.tipo {
		case 1: // ELEIÇÃO:
			//Quem tem o maior ID até o momento é o iniciador
			fmt.Printf("%2d: Recebi ELEIÇÃO (Candidato: %d, Iniciador: %d)\n", TaskId, temp.corpo[0], temp.corpo[1])

			// Se eu sou o iniciador e a mensagem deu a volta
			if temp.corpo[1] == TaskId && jaInicieiEleicao { //Condição de parada
				fmt.Printf("%2d: Eleição concluída! Vencedor: %d\n", TaskId, temp.corpo[0])
				actualLeader = temp.corpo[0]

				// Finaliza a eleição (anuncia o novo líder para o anel e reseta o estado)
				out <- mensagem{tipo: 5, corpo: [6]int{actualLeader}}
				jaInicieiEleicao = false
			} else {
				// Se eu sou maior que o candidato atual, eu viro o candidato
				if TaskId > temp.corpo[0] { //
					temp.corpo[0] = TaskId
				}
				// Se eu for o iniciador pela primeira vez, mudo o estado para eleição
				if temp.corpo[1] == TaskId {
					jaInicieiEleicao = true
				}
				out <- temp
			}

		case 5: // NOVO LÍDER (Faze de Coordenação)
			if actualLeader != temp.corpo[0] { // Atualiza a memória com o ID do novo lider
				actualLeader = temp.corpo[0]
				fmt.Printf("%2d: Líder atualizado para %d\n", TaskId, actualLeader)
				out <- temp // Repassa a noticia para o proximo "vizinho"
			}

		case 2: // FALHA (Simulação de Erro)
			bFailed = true //Apenas repassa mensagens sem processar
			fmt.Printf("%2d: Entrei em falha. Liíder Atual %d\n", TaskId, actualLeader)
			controle <- TaskId //Confirma ao controlador que a instrução de falha foi recebida

		case 4: // ENCERRAR
			fmt.Printf("%2d: Encerrando\n", TaskId)
			return
		}
	}
}

func main() {
	wg.Add(5)
	go ElectionStage(0, chans[3], chans[0], 0)
	go ElectionStage(1, chans[0], chans[1], 0)
	go ElectionStage(2, chans[1], chans[2], 0)
	go ElectionStage(3, chans[2], chans[3], 0)

	fmt.Println("\n   Anel de processos criado")
	go ElectionControler(controle)
	wg.Wait()
}
