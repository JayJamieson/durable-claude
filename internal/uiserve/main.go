// Command uiserve serves the static viewer during development, so the page can
// be exercised without publishing it to GitHub Pages first.
package main

import (
	"flag"
	"log"
	"net/http"
)

func main() {
	dir := flag.String("dir", "docs", "directory to serve")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	log.Printf("serving %s on http://localhost%s", *dir, *addr)
	log.Fatal(http.ListenAndServe(*addr, http.FileServer(http.Dir(*dir))))
}
