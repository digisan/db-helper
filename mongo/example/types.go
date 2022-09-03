package example

import (
	"fmt"
	"strings"
)

type Person struct {
	FullName string
	Age      int
	Class    struct {
		Name    string
		Teacher string
	}
}

func (p Person) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintln("FullName:", p.FullName))
	sb.WriteString(fmt.Sprintln("Age:", p.Age))
	sb.WriteString(fmt.Sprintln("Class.Name:", p.Class.Name))
	sb.WriteString(fmt.Sprintln("Class.Teacher:", p.Class.Teacher))
	return sb.String()
}
