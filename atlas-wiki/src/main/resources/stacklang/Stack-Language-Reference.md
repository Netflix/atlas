
Reference for operations available in the stack language. Use the sidebar to navigate.

```wiki.script
import com.netflix.atlas.wiki._
val w2v = (vocab :: vocab.dependencies).flatMap(v => v.words.map(_ -> v.name)).toMap
val words = vocab.allWords.groupBy(_.name)
val sections = words.toList.sortWith(_._1 < _._1).map { case (name, ws) =>
  val items = ws.map { w =>
    val vocabName = w2v(w)
    s"* [${w.signature}]($vocabName-${Utils.fileName(name)})"
  }
  s"#### $name\n\n${items.mkString("\n")}\n"
}
sections.mkString("\n")
```
