package main

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/executor"
	"garakutadb/parser"
	"garakutadb/planner"
	"garakutadb/storage"
	"github.com/chzyer/readline"
	"strings"
)

func main() {
	rl, err := readline.New("GarakutaDB > ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	history := make([]string, 0)

	for {
		line, err := rl.Readline()
		if err != nil { // io.EOFが返されると終了
			break
		}
		if line == "exit" {
			break
		}
		if strings.TrimSpace(line) == "" {
			continue
		}

		// 実行されたコマンドを履歴に追加
		history = append(history, line)
		rl.SaveHistory(line)

		err = Execute(line)
		if err != nil {
			fmt.Println(err)
		}
	}

	fmt.Println("Bye!")
}

func Execute(sqlString string) error {
	ps := parser.NewSimpleParser()
	stmt, err := ps.Parse(sqlString)
	if err != nil {
		return err
	}

	dm := storage.NewDiskManager("data")
	st := storage.NewStorage(dm)
	ct, err := catalog.LoadCatalog(st)
	if err != nil {
		return err
	}

	sp := planner.NewSimplePlanner(ct)
	plan, err := sp.MakePlan(stmt)
	if err != nil {
		return err
	}

	ex := executor.NewSimpleExecutor(ct, st)

	trMgr := storage.NewTransactionManager(st)
	rs, err := ex.Execute(plan, trMgr.Begin(), trMgr)
	if err != nil {
		return err
	}

	// 各列の最大文字数を計算
	columnWidths := make([]int, len(rs.Header))
	for i, header := range rs.Header {
		columnWidths[i] = len(header)
	}

	for _, row := range rs.Rows {
		for i, cell := range row {
			columnWidths[i] = max(columnWidths[i], len(cell))
		}
	}

	// ヘッダーの出力
	headerParts := make([]string, len(rs.Header))
	for i, header := range rs.Header {
		headerParts[i] = fmt.Sprintf("%-*s", columnWidths[i], header)
	}
	if len(rs.Header) > 0 {
		fmt.Println(strings.Join(headerParts, " | "))
	}

	// 仕切りの出力
	separatorParts := make([]string, len(rs.Header))
	for i, width := range columnWidths {
		separatorParts[i] = strings.Repeat("-", width)
	}
	if len(rs.Header) > 0 {
		fmt.Println(strings.Join(separatorParts, "-+-"))
	}

	// 行の出力
	for _, row := range rs.Rows {
		rowParts := make([]string, len(row))
		for i, cell := range row {
			rowParts[i] = fmt.Sprintf("%-*s", columnWidths[i], cell)
		}
		fmt.Println(strings.Join(rowParts, " | "))
	}

	fmt.Println(rs.Message)
	fmt.Println()

	return nil
}
