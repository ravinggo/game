The golang type data is determined at compile time 
eface will directly point to the corresponding type of pointer

the address of the type data is used as a unique identifier
For more details, see **_reflect.Typeof_** and **_go tool compile -S_**
```
var a interface{} = (*T)(nil)
typPtr := *(*uintptr)(unsafe.Pointer(&a))
```