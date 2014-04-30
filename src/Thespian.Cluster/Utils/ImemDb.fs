namespace Nessos.Thespian.ImemDb

open System
open System.Collections
open System.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape
open Microsoft.FSharp.Reflection


[<CustomEquality>][<CustomComparison>]
type Attribute = {
    RelationName: string
    Property: PropertyInfo
} with
    member a.Name = a.RelationName + "." + a.Property.DeclaringType.FullName + "." + a.RelationName + "." + a.Property.PropertyType.FullName

    member a.RelationId = Attribute.MakeRelationId(a.RelationName, a.Property.DeclaringType)

    static member MakeRelationId(name: string, recordType: Type) = name //recordType.Name + ":" + name

    member private a.CompareTo(other: obj) =
        match other with
        | :? Attribute as otherAttr -> a.Name.CompareTo(otherAttr.Name)
        | _ -> invalidArg "Argument in comparison not of Attribute type" "other"

    override a.GetHashCode() = a.Name.GetHashCode()
    override a.Equals(other: obj) = a.CompareTo(other) = 0

    interface IComparable with
        member a.CompareTo(other: obj) = a.CompareTo(other)

type Index<'T> = { 
    IndexName: string
    Attribute: Attribute
    IndexKey: 'T -> IComparable
    IndexMap: Map<IComparable, Set<IComparable>> 
}

and Table<'T> = { 
    Name: string
    PrimaryKey: 'T -> IComparable
    DataMap: Map<IComparable, 'T>
    Indexes: Map<string, Index<'T>>
} with
    override tbl.ToString() =
        let data = tbl.DataMap |> Map.toList
        sprintf "%A, %A :: %A" (tbl.GetType().Name) tbl.Name data

and SigmaBinaryOp = And | Or | Eq | Lt | Gt

and SigmaUnaryOp = Not 

and AttributeDereference = Direct of Attribute * (SigmaExpr list (* GeneralExpr list *))
                           | Indirect of AttributeDereference * Expr

and SigmaAtom = Top //true
                | Bottom //false
                //| Attr of Attribute
                //| AttrDeref of Attribute * Expr
                | AttrDeref of AttributeDereference
                | RelationVar of string //relation id
                | Literal of Expr //value expr

and SigmaExpr = Atom of SigmaAtom
                | BinaryExpr of SigmaBinaryOp * SigmaExpr * SigmaExpr
                | UnaryExpr of SigmaUnaryOp * SigmaExpr
                | GeneralExpr of Expr * (Var * SigmaAtom) list
                with
                    override s.ToString() = 
                        match s with
                        | Atom atom -> sprintf "Atom(%A)" atom
                        | BinaryExpr(op, left, right) -> sprintf "BinaryExpr(%A, %A, %A)" op left right
                        | UnaryExpr(op, expr) -> sprintf "UnaryExpr(%A, %A)" op expr
                        | GeneralExpr(expr, _) -> sprintf "GeneralExpr(%A)" expr

and QueryType = RecordType of string * Type
                | Nullable of QueryType
                | JoinType of QueryType * QueryType
                with
                    member qt.GetRecordType() =
                        match qt with
                        | RecordType(_, rt) -> rt
                        | Nullable qt -> qt.GetNullable()
                        | JoinType(leftQueryType, rightQueryType) ->
                            FSharpType.MakeTupleType([| leftQueryType.GetRecordType(); rightQueryType.GetRecordType() |])

                    member qt.GetNullable() = typedefof<_ option>.MakeGenericType([| qt.GetRecordType() |])

and TableExpr = {
    Name: string
    Expr: Expr
    QueryType: QueryType
    Cardinality: int
    IndexedAttributes: Map<Attribute, string>
} with 
    member qe.RecordType = qe.Expr.Type.GetGenericArguments().[0]

and RelationalExpr =
    | Relation of TableExpr
    | Sigma of SigmaExpr * RelationalExpr
    | Product of RelationalExpr * RelationalExpr
    | InnerJoin of SigmaExpr * RelationalExpr * RelationalExpr
    | LeftOuterJoin of SigmaExpr * RelationalExpr * RelationalExpr
    | RightOuterJoin of SigmaExpr * RelationalExpr * RelationalExpr
    //| NaturalJoin of RelationalExpr * RelationalExpr
    //| EquiJoin of RelationalExpr * Column * RelationalExpr * Column
    with
        member re.GetQueryShape() =
            match re with
            | Relation tableExpr -> TableValue(tableExpr.Expr)
            | Sigma(_, re) -> re.GetQueryShape()
            | Product(left, right) 
            | LeftOuterJoin(_, left, right)
            | RightOuterJoin(_, left, right)
            | InnerJoin(_, left, right) -> JoinValue(left.GetQueryShape(), right.GetQueryShape())

and QueryShape = TableValue of Expr
                 | JoinValue of QueryShape * QueryShape

and Query<'T> = {
    RelationalQuery: RelationalExpr
    Compiler: RelationalExpr -> seq<'T>
}

module Query =

    type JoinMode = Inner | LeftOuter | RightOuter

    type AccessPath =
        | RelationScan of SigmaExpr * TableExpr
        | IndexEq of string * Expr (* IComparable value *) * TableExpr
        | SequenceScan of SigmaExpr * AccessPath
        | PathJoin of SigmaExpr * JoinMode * AccessPath * AccessPath
        | IndexedEqJoin of Attribute * string * Type * string * AccessPath * AccessPath
        with
            member ap.GetQueryType() = 
                match ap with
                | RelationScan(_, tableExpr) 
                | IndexEq(_, _, tableExpr) -> tableExpr.QueryType
                | SequenceScan(_, ap') -> ap'.GetQueryType()
                | IndexedEqJoin(_, _, _, _, apLeft, apRight)
                | PathJoin(_, Inner, apLeft, apRight) -> JoinType(apLeft.GetQueryType(), apRight.GetQueryType())
                | PathJoin(_, LeftOuter, apLeft, apRight) -> JoinType(apLeft.GetQueryType(), Nullable <| apRight.GetQueryType())
                | PathJoin(_, RightOuter, apLeft, apRight) -> JoinType(Nullable <| apLeft.GetQueryType(), apRight.GetQueryType())

            member ap.Cardinality =
                match ap with
                | RelationScan(_, tableExpr) -> tableExpr.Cardinality
                | IndexEq _ -> 1
                | SequenceScan(_, accessPath) -> accessPath.Cardinality
                | IndexedEqJoin(_, _, _, _, apLeft, apRight)
                | PathJoin(_, _, apLeft, apRight) -> apLeft.Cardinality * apRight.Cardinality


    let rec private (|AtomicProposition|_|) expr =
        match expr with
        | BinaryExpr(And, _, _)
        | BinaryExpr(Or, _, _) -> None
        | UnaryExpr(Not, AtomicProposition _)
        | GeneralExpr _
        | _ -> Some expr

    let private recordTypeOfExpr (expr: Expr) = expr.Type.GetGenericArguments().[0]

    let private relationAttributes (tableExpr: TableExpr) =
        tableExpr.QueryType.GetRecordType().GetProperties() |> Seq.map (fun pinfo -> { RelationName = tableExpr.Name; Property = pinfo }) |> Set.ofSeq

    let rec private relationalExprAttributes = 
        function Relation relation -> relationAttributes relation
                 | Sigma(_, rel) -> relationalExprAttributes rel
                 | InnerJoin(_, left, right)
                 | LeftOuterJoin(_, left, right)
                 | RightOuterJoin(_, left, right)
                 | Product(left, right) -> relationalExprAttributes right |> Set.union (relationalExprAttributes left)

    let rec private (|AttrDerefAttribute|) =
            function Direct(attribute, _) -> attribute
                     | Indirect(AttrDerefAttribute attribute, _) -> attribute

    let rec private sigmaAttributes =
        function Atom(AttrDeref(AttrDerefAttribute attribute)) -> Set.singleton attribute
                 | BinaryExpr(_, left, right) -> sigmaAttributes right |> Set.union (sigmaAttributes left)
                 | UnaryExpr(_, expr) -> sigmaAttributes expr
                 | _ -> Set.empty

    let private (|SigmaAttributes|) = sigmaAttributes

    let private cnf =
        let rec deMorganSimplify =
            function Atom(Top | Bottom) as atom -> atom
                     | UnaryExpr(Not, Atom Top) -> Atom Bottom
                     | UnaryExpr(Not, Atom Bottom) -> Atom Top
                     | UnaryExpr(Not, UnaryExpr(Not, expr)) -> deMorganSimplify <| expr
                     | UnaryExpr(Not, BinaryExpr(And, left, right)) -> BinaryExpr(Or, deMorganSimplify <| UnaryExpr(Not, left), deMorganSimplify <| UnaryExpr(Not, right))
                     | UnaryExpr(Not, BinaryExpr(Or, left, right)) -> BinaryExpr(And, deMorganSimplify <| UnaryExpr(Not, left), deMorganSimplify <| UnaryExpr(Not, right))
                     | UnaryExpr(Not, _) as uexpr -> uexpr
                     | BinaryExpr(And | Or as lop, left, right) -> BinaryExpr(lop, deMorganSimplify <| left, deMorganSimplify <| right)
                     | BinaryExpr _ as bexpr -> bexpr
                     | GeneralExpr _ as gexpr -> gexpr
                     | _ -> failwith "Compiler internal error: Expression not boolean"

        let rec distributeDisjunction left right =
            let rec distributeDisjunction' left right =
                match right with
                | BinaryExpr((And | Or), AtomicProposition left', AtomicProposition right') -> BinaryExpr(And, BinaryExpr(Or, left, left'), BinaryExpr(Or, left, right'))
                | BinaryExpr((And | Or), AtomicProposition left', right') -> BinaryExpr(And, BinaryExpr(Or, left, left'), distributeDisjunction' left right')
                | BinaryExpr((And | Or), left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction' left left', BinaryExpr(Or, left, left'))
                | BinaryExpr((And | Or), left', right') -> BinaryExpr(And, distributeDisjunction' left left', distributeDisjunction' left right')
                | AtomicProposition _ -> BinaryExpr(Or, left, right)
                | _ -> failwith "Compiler error: Expression not boolean"

            match left with
            | BinaryExpr((And | Or), AtomicProposition left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction' left' right, distributeDisjunction right' right)
            | BinaryExpr((And | Or), AtomicProposition left', right') -> BinaryExpr(And, distributeDisjunction' left' right, distributeDisjunction right' right)
            | BinaryExpr((And | Or), left', AtomicProposition right') -> BinaryExpr(And, distributeDisjunction left' right, distributeDisjunction' right' right)
            | BinaryExpr((And | Or), left', right') -> BinaryExpr(And, distributeDisjunction left' right, distributeDisjunction right' right)
            | AtomicProposition _ -> distributeDisjunction' left right
            | _ -> failwith "Compiler error: Expression not boolean"

        let rec normalize =
            function BinaryExpr(And, left, right) -> BinaryExpr(And, normalize left, normalize right)
                     | BinaryExpr(Or, left, right) -> distributeDisjunction (normalize left) (normalize right)
                     | AtomicProposition p -> p
                     | _ -> failwith "CompilerError: Expression not boolean"

        normalize << deMorganSimplify

    let rec private topBottomNormalize expr =
        let normalize = function UnaryExpr(Not, Atom Top) -> Atom Bottom
                                 | UnaryExpr(Not, Atom Bottom) -> Atom Top
                                 | UnaryExpr _ as e -> e
                                 | BinaryExpr(And, Atom Bottom, _)
                                 | BinaryExpr(And, _, Atom Bottom) -> Atom Bottom
                                 | BinaryExpr(And, Atom Top, right) -> right
                                 | BinaryExpr(And, left, Atom Top) -> left
                                 | BinaryExpr(Or, Atom Top, _)
                                 | BinaryExpr(Or, _, Atom Top) -> Atom Top
                                 | BinaryExpr(Or, Atom Bottom, right) -> right
                                 | BinaryExpr(Or, left, Atom Bottom) -> left
                                 | BinaryExpr _ as e -> e
                                 | _ as e -> e
        match expr with
        | Atom _ as a -> a
        | UnaryExpr(Not, Atom Top) -> Atom Bottom
        | UnaryExpr(Not, Atom Bottom) -> Atom Top
        | UnaryExpr(Not, expr) -> normalize <| UnaryExpr(Not, topBottomNormalize expr)
        | BinaryExpr(And, Atom Bottom, _)
        | BinaryExpr(And, _, Atom Bottom) -> Atom Bottom
        | BinaryExpr(And, Atom Top, right) -> topBottomNormalize right
        | BinaryExpr(And, left, Atom Top) -> topBottomNormalize left
        | BinaryExpr(And, left, right) -> normalize <| BinaryExpr(And, topBottomNormalize left, topBottomNormalize right)
        | BinaryExpr(Or, Atom Top, _)
        | BinaryExpr(Or, _, Atom Top) -> Atom Top
        | BinaryExpr(Or, Atom Bottom, right) -> topBottomNormalize right
        | BinaryExpr(Or, left, Atom Bottom) -> topBottomNormalize left
        | BinaryExpr(Or, left, right) -> normalize <| BinaryExpr(Or, topBottomNormalize left, topBottomNormalize right)
        | BinaryExpr _ as e -> e
        | GeneralExpr _ as e -> e

    let rec private relationsInAccessPath ap = seq {
        match ap with
        | RelationScan(_, rel)
        | IndexEq(_, _, rel) -> yield rel
        | SequenceScan(_, ap) -> yield! relationsInAccessPath ap
        | IndexedEqJoin(_, _, _, _, (RelationScan(_, rel) | IndexEq(_, _, rel)), other)
        | IndexedEqJoin(_, _, _, _, other, (RelationScan(_, rel) | IndexEq(_, _, rel)))
        | PathJoin(_, _, (RelationScan(_, rel) | IndexEq(_, _, rel)), other)
        | PathJoin(_, _, other, (RelationScan(_, rel) | IndexEq(_, _, rel))) ->
            yield rel
            yield! relationsInAccessPath other
        | IndexedEqJoin(_, _, _, _, left, right)
        | PathJoin(_, _, left, right) ->
            yield! relationsInAccessPath left
            yield! relationsInAccessPath right
    }

    let rec private cnfList =
        function AtomicProposition p -> [p]
                 | BinaryExpr(And, left, right) -> (cnfList left)@(cnfList right)
                 | _ as p -> [p]

    let rec private translate relExpr =
        match relExpr with
        | Relation tableExpr -> RelationScan(Atom Top, tableExpr)
        | Product(left, right) -> PathJoin(Atom Top, Inner, translate left, translate right)
        | InnerJoin(joinCondition, left, right) -> PathJoin(joinCondition, Inner, translate left, translate right)
        | LeftOuterJoin(joinCondition, left, right) -> PathJoin(joinCondition, LeftOuter, translate left, translate right)
        | RightOuterJoin(joinCondition, left, right) -> PathJoin(joinCondition, RightOuter, translate left, translate right)
        | Sigma(sigma, expr) -> 
            let sigCnf = cnf sigma
            SequenceScan(sigCnf, translate expr) //sigmaTranslate (cnf sigma) (translate expr)

    let private accessPathOptimize accessPath =
        let rec scanConditions = 
            function RelationScan(pred, _) -> cnfList pred
                     | SequenceScan(pred, ap) -> (cnfList pred)@(scanConditions ap)
                     | PathJoin(joinCondition, _, left, right) -> (cnfList joinCondition)@(scanConditions left)@(scanConditions right)
                     | _ -> []

        let atomicPropositonCost (tableExpr: TableExpr) =
            function Atom Bottom 
                     | BinaryExpr(Eq, Atom(Literal _), Atom(Literal _)) -> 0
                     | BinaryExpr(Eq, Atom(AttrDeref(AttrDerefAttribute attribute)), Atom(Literal _))
                     | BinaryExpr(Eq, Atom(Literal _), Atom(AttrDeref(AttrDerefAttribute attribute))) ->
                        if tableExpr.IndexedAttributes.ContainsKey attribute then 1/10
                        else 1
                     | _ -> System.Int32.MaxValue

        let rec (|NonJoin|_|) accessPath =
            match accessPath with
            | RelationScan _
            | IndexEq _
            | SequenceScan(_, NonJoin _) -> Some accessPath
            | _ -> None

        let rec joinPairs = 
            function PathJoin(condition, Inner, (NonJoin _ as left), (NonJoin _ as right)) -> [PathJoin(condition, Inner, left, right); PathJoin(condition, Inner, right, left)]
                     | PathJoin(condition, LeftOuter, (NonJoin _ as left), (NonJoin _ as right)) -> [PathJoin(condition, LeftOuter, left, right); PathJoin(condition, RightOuter, right, left)]
                     | PathJoin(condition, RightOuter, (NonJoin _ as left), (NonJoin _ as right)) -> [PathJoin(condition, RightOuter, left, right); PathJoin(condition, LeftOuter, right, left)]
                     | PathJoin(condition, Inner, (PathJoin _ as join), (NonJoin _ as relation)) ->
                        joinPairs join |> List.map (fun pair -> [PathJoin(condition, Inner, relation, pair); PathJoin(condition, Inner, pair, relation)]) |> List.concat
                     | PathJoin(condition, LeftOuter, (PathJoin _ as join), (NonJoin _ as relation)) -> 
                        joinPairs join |> List.map (fun pair -> [PathJoin(condition, LeftOuter, relation, pair); PathJoin(condition, RightOuter, pair, relation)]) |> List.concat
                     | PathJoin(condition, RightOuter, (PathJoin _ as join), (NonJoin _ as relation)) -> 
                        joinPairs join |> List.map (fun pair -> [PathJoin(condition, RightOuter, relation, pair); PathJoin(condition, LeftOuter, pair, relation)]) |> List.concat
                     | SequenceScan(_, path) -> joinPairs path
                     | _ -> failwith "Invalid initial access path."

        let joinCost =
            function PathJoin(BinaryExpr(Eq, Atom(AttrDeref(AttrDerefAttribute firstAttribute)), Atom(AttrDeref(AttrDerefAttribute secondAttribute))), _, selectedPath, joinCandidate) ->
                        let selectedRelations = relationsInAccessPath selectedPath
                        let candidateRelations = relationsInAccessPath joinCandidate

                        if selectedRelations |> Seq.exists (fun relation -> relation.IndexedAttributes.ContainsKey firstAttribute || relation.IndexedAttributes.ContainsKey secondAttribute) then
                            joinCandidate.Cardinality
                        else if candidateRelations |> Seq.exists (fun relation -> relation.IndexedAttributes.ContainsKey firstAttribute || relation.IndexedAttributes.ContainsKey secondAttribute) then
                            selectedPath.Cardinality
                        else 
                            (selectedPath.Cardinality * joinCandidate.Cardinality)/(joinCandidate.Cardinality/2)
                    | PathJoin(_, _, selectedPath, joinCandidate) ->
                        selectedPath.Cardinality * joinCandidate.Cardinality
                    | _ -> System.Int32.MaxValue

        let singleRelationAccessPath (relation: TableExpr) (relationSigmaConjuncts: SigmaExpr list) =
            if List.isEmpty relationSigmaConjuncts then
                RelationScan(Atom Top, relation)
            else
                let sortedConditions = relationSigmaConjuncts |> List.sortBy (atomicPropositonCost relation)

                sortedConditions |> List.fold (fun accessPath condition ->
                        match accessPath, condition with
                        | None, BinaryExpr(Eq, Atom(AttrDeref(AttrDerefAttribute attribute)), Atom(Literal literal))
                        | None, BinaryExpr(Eq, Atom(Literal literal), Atom(AttrDeref(AttrDerefAttribute attribute))) ->
                            match relation.IndexedAttributes.TryFind attribute with
                            | Some idxName -> Some <| IndexEq(idxName, literal, relation) //NEED TO FIX THIS; the attribute dereference could be indirect, which requires different compilation than a direct dereference
                            | None -> Some <| RelationScan(condition, relation)
                        | None, _ ->
                            Some <| RelationScan(condition, relation)
                        | Some sequencePath, _ ->
                            Some <| SequenceScan(condition, sequencePath)
                    ) None
                |> Option.get

        let sigmaConjunctsOfRelation (conjuncts: SigmaExpr list) (relation: TableExpr) =
            let relationAttributes = relationAttributes relation
            conjuncts |> List.filter (fun conjunct -> Set.isSubset (sigmaAttributes conjunct) relationAttributes)

        let scanConjuncts = 
            scanConditions accessPath |> List.filter (fun conjunct -> conjunct <> Atom Top)
        let relations = relationsInAccessPath accessPath |> Seq.toList
//        let finalScanConjuncts =
//            scanConjuncts |> List.filter (fun conjunct -> let attributes = sigmaAttributes conjunct in not(relations |> List.exists (fun relation -> Set.isSubset attributes (relationAttributes relation))))
        let finalScanConjuncts = scanConjuncts |> List.filter (fun conjunct -> Set.isEmpty (sigmaAttributes conjunct))

        let combineConjuncts = List.fold (fun acc conjunct -> BinaryExpr(And, acc, conjunct)) (Atom Top)

        let rec singleOptimize =
            function | RelationScan(_, relation) -> singleRelationAccessPath relation (sigmaConjunctsOfRelation scanConjuncts relation)
                     | SequenceScan(_, path) -> singleOptimize path
                     | _ -> failwith "Optimizer internal failure."

        let rec joinOptimize (accessPath: AccessPath) =
            match accessPath with
            | RelationScan(_, relation) -> 
                singleRelationAccessPath relation (sigmaConjunctsOfRelation scanConjuncts relation)
            | SequenceScan(_, path) -> joinOptimize path
            | _ ->
                let pairs = joinPairs accessPath
                let costSorted = pairs |> List.sortBy joinCost
                match List.head costSorted with
                | PathJoin(BinaryExpr(Eq, Atom(AttrDeref(AttrDerefAttribute firstAttribute)), Atom(AttrDeref(AttrDerefAttribute secondAttribute))) as condition, joinMode, selectedPath, joinCandidate) ->
                    let first = joinOptimize selectedPath
                    let second = joinOptimize joinCandidate

                    let pickF = fun relation ->  match relation.IndexedAttributes.TryFind firstAttribute with Some attribute -> Some(relation.Name, relation.RecordType, attribute) | _ -> None

                    match relationsInAccessPath second |> Seq.tryPick pickF with
                    | Some(relationName, recordType, idxName) -> IndexedEqJoin(secondAttribute, relationName, recordType, idxName, first, second)
                    | None ->
                        match relationsInAccessPath first |> Seq.tryPick pickF with
                        | Some(relationName, recordType, idxName) -> IndexedEqJoin(firstAttribute, relationName, recordType, idxName, first, second)
                        | None -> PathJoin(condition, joinMode, first, second)
                | p -> p

        let path = match accessPath with
                   | NonJoin _ -> singleOptimize accessPath
                   | _ -> joinOptimize accessPath
        
        //let path = singleOptimize accessPath
        if finalScanConjuncts |> List.isEmpty then path
        else SequenceScan(combineConjuncts finalScanConjuncts, path)

    let private compile relExpr =
        let buildLambdaBodyBindings queryType =
            let env = ref Map.empty

            let rec buildBindings path queryType = 
                match queryType with
                | JoinType((Nullable(RecordType(leftName, _)) as left), (RecordType(rightName, _) as right))
                | JoinType((RecordType(leftName, _) as left), (Nullable(RecordType(rightName, _)) as right))
                | JoinType((Nullable(RecordType(leftName, _)) as left), (Nullable(RecordType(rightName, _)) as right))
                | JoinType((RecordType(leftName, _) as left), (RecordType(rightName, _) as right)) ->
                    let leftType = left.GetRecordType()
                    let rightType = right.GetRecordType()

                    let leftVar = Var(sprintf "%sl" path, leftType, false)
                    let leftExpr = Expr.Var leftVar
                    let rightVar = Var(sprintf "%sr" path, rightType, false)
                    let rightExpr = Expr.Var rightVar

                    let leftId = Attribute.MakeRelationId(leftName, leftType)
                    let rightId = Attribute.MakeRelationId(rightName, rightType)

                    env := !env |> Map.add leftId leftExpr |> Map.add rightId rightExpr

                    fun arg body -> Expr.Let(leftVar, Expr.TupleGet(arg, 0), Expr.Let(rightVar, Expr.TupleGet(arg, 1), body))
                | JoinType((Nullable(RecordType(leftName, _)) as left), (JoinType _ as rightType))
                | JoinType((RecordType(leftName, _) as left), (JoinType _ as rightType)) ->
                    let leftType = left.GetRecordType()
                    let leftVar = Var(sprintf "%sl" path, leftType, false)
                    let leftExpr = Expr.Var leftVar
                    let rightVar = Var(sprintf "%sr" path, rightType.GetRecordType(), false)
                    let rightExpr = Expr.Var rightVar

                    let leftId = Attribute.MakeRelationId(leftName, leftType)

                    env := !env |> Map.add leftId leftExpr

                    let rightBindingsBuilder = buildBindings (path + "r") rightType

                    fun arg body -> 
                        Expr.Let(leftVar, Expr.TupleGet(arg, 0), Expr.Let(rightVar, Expr.TupleGet(arg, 1), rightBindingsBuilder rightExpr body))
                | JoinType((JoinType _ as leftType), (Nullable((RecordType(rightName, _))) as right))
                | JoinType((JoinType _ as leftType), ((RecordType(rightName, _)) as right)) ->
                    let rightType = right.GetRecordType()
                    let leftVar = Var(sprintf "%sl" path, leftType.GetRecordType(), false)
                    let leftExpr = Expr.Var leftVar
                    let rightVar = Var(sprintf "%sr" path, rightType, false)
                    let rightExpr = Expr.Var rightVar

                    let rightId = Attribute.MakeRelationId(rightName, rightType)

                    env := !env |> Map.add rightId rightExpr

                    let leftBindingsBuilder = buildBindings (path + "l") leftType

                    fun arg body -> 
                        Expr.Let(rightVar, Expr.TupleGet(arg, 1), Expr.Let(leftVar, Expr.TupleGet(arg, 0), leftBindingsBuilder leftExpr body))
                | JoinType((JoinType _ as leftType), (JoinType _ as rightType)) ->
                    let leftVar = Var(sprintf "%sl" path, leftType.GetRecordType(), false)
                    let leftExpr = Expr.Var leftVar
                    let rightVar = Var(sprintf "%sr" path, rightType.GetRecordType(), false)
                    let rightExpr = Expr.Var rightVar

                    let leftBindingsBuilder = buildBindings (path + "l") leftType
                    let rightBindingsBuilder = buildBindings (path + "r") rightType

                    fun arg body ->
                        Expr.Let(
                            leftVar, 
                            Expr.TupleGet(arg, 0), 
                            leftBindingsBuilder leftExpr (Expr.Let(rightVar, Expr.TupleGet(arg, 1), rightBindingsBuilder rightExpr body)))
                | _ -> failwith "Compiler internal error"

            let bindingsBuilder = buildBindings "" queryType

            !env, bindingsBuilder

        let compileSigma sigma (queryType: QueryType) =
            let rec (|AttrDerefExpr|) env =
                function Direct(attribute, args) ->
                            Expr.PropertyGet(env |> Map.find attribute.RelationId, attribute.Property, args |> List.map (compileLambdaBody env))
                         | Indirect(AttrDerefExpr env attrExpr, expr) ->
                            if expr.GetFreeVars() |> Seq.length > 1 then failwith "Compiler error: Unable to compile join/where clause expression."
                            expr.Substitute(fun _ -> Some attrExpr)


            and (|AtomExpr|) env =
                function AttrDeref(AttrDerefExpr env attributeExpr) -> attributeExpr //Attr attribute -> Expr.PropertyGet(env |> Map.find (attribute.RelationId), attribute.Property)
//                         | AttrDeref(attribute, expr) ->
//                            if expr.GetFreeVars() |> Seq.length > 1 then failwith "Compiler error: Unable to compile join/where clause expression."
//                            expr.Substitute(fun _ -> let (AtomExpr env expr) = Attr attribute in Some expr)//env.TryFind attribute.RelationId)
                         | RelationVar relationId -> env |> Map.find relationId
                         | Literal valueExpr -> valueExpr
                         | Bottom -> Expr.Value false
                         | Top -> Expr.Value true

            and binaryOpExpr left right = 
                function Eq -> let left', right' = Expr.Coerce(left, typeof<obj>), Expr.Coerce(right, typeof<obj>) in <@@ (%%left' = %%right') @@>
                        | Lt -> let left', right' = Expr.Coerce(left, typeof<IComparable>), Expr.Coerce(right, typeof<IComparable>) in <@@ (%%left' < %%right') @@>
                        | Gt -> let left', right' = Expr.Coerce(left, typeof<IComparable>), Expr.Coerce(right, typeof<IComparable>) in <@@ (%%left' > %%right') @@>
                        | And -> let left', right' = Expr.Coerce(left, typeof<bool>), Expr.Coerce(right, typeof<bool>) in <@@ (%%left && %%right) @@>
                        | Or -> let left', right' = Expr.Coerce(left, typeof<bool>), Expr.Coerce(right, typeof<bool>) in <@@ (%%left || %%right) @@>

            and unaryOpExpr expr =
                function Not -> <@@ (not (%%expr)) @@>

            and compileLambdaBody env = 
                    function BinaryExpr(op, Atom(AtomExpr env left), Atom(AtomExpr env right)) ->
                                binaryOpExpr left right op
                             | BinaryExpr(op, Atom(AtomExpr env left), right) ->
                                binaryOpExpr left (compileLambdaBody env right) op
                             | BinaryExpr(op, left, Atom(AtomExpr env right)) ->
                                binaryOpExpr (compileLambdaBody env left) right op
                             | BinaryExpr(op, left, right) ->
                                binaryOpExpr (compileLambdaBody env left) (compileLambdaBody env right) op
                             | UnaryExpr(op, Atom(AtomExpr env expr)) ->
                                unaryOpExpr expr op
                             | UnaryExpr(op, sigma) ->
                                unaryOpExpr (compileLambdaBody env sigma) op
                             | Atom(AtomExpr env expr) -> expr
                             | GeneralExpr(expr, substitutes) ->
                                let varMap = substitutes |> Map.ofList
                                expr.Substitute(fun var -> match varMap |> Map.tryFind var with Some(AtomExpr env expr) -> Some expr | _ -> None)

            let recordType = queryType.GetRecordType()

            let argumentVar = Var("records", recordType, false)
            let argumentExpr = Expr.Var argumentVar

            match queryType with
            | RecordType(name, recordType) ->
                let env = Map.empty |> Map.add (Attribute.MakeRelationId(name, recordType)) argumentExpr
                //let env = Map.empty |> Map.add name argumentExpr
                Expr.Lambda(argumentVar, compileLambdaBody env sigma)
            | JoinType _ ->
                let env, bindingsBuilder = buildLambdaBodyBindings queryType

                Expr.Lambda(argumentVar, bindingsBuilder argumentExpr (compileLambdaBody env sigma))
            | _ -> failwith "Compiler internal error."

        let compileFinalMap (resultQueryType: QueryType) (finalQueryType: QueryType) =
            let recordType = resultQueryType.GetRecordType()

            let argumentVar = Var("records", recordType, false)
            let argumentExpr = Expr.Var argumentVar

            match resultQueryType with
            | RecordType _ -> Expr.Lambda(argumentVar, argumentExpr)
            | JoinType _ ->
                let env, bindingsBuilder = buildLambdaBodyBindings resultQueryType

                let rec argsMap queryType =
                    match queryType with
                    | Nullable(RecordType(name, recordType))
                    | RecordType(name, recordType) ->
                        Map.find (Attribute.MakeRelationId(name, recordType)) env
                    | JoinType(leftType, rightType) ->
                        
                        let leftExpr = argsMap leftType
                        let rightExpr = argsMap rightType

                        Expr.NewTuple [leftExpr; rightExpr]
                    | _ -> failwith "Compiler internal error."

                Expr.Lambda(argumentVar, bindingsBuilder argumentExpr (argsMap finalQueryType))
            | _ -> failwith "Compiler internal error."

        let getEmptySeqExpr seqType =
            match <@ Seq.empty<int> @> with
            | Call(_, mInfo, _) -> Expr.Call(mInfo.GetGenericMethodDefinition().MakeGenericMethod([| seqType |]), []) //Seq.empty<'T> & typeof<'T> = seqType
            | _ -> failwith "Compiler error."

        let seqIsEmptyMethodInfo seqType =
            match <@ Seq.isEmpty Seq.empty<int> @> with
            | Call(_, mInfo, _) -> mInfo.GetGenericMethodDefinition().MakeGenericMethod([| seqType |])
            | _ -> failwith "Compiler error."

        let seqSingletonMethodInfo seqType =
            match <@ Seq.singleton<int> @> with
            | Call(_, mInfo, _) -> mInfo.GetGenericMethodDefinition().MakeGenericMethod([| seqType |])
            | _ -> failwith "Compiler error."

        let seqMapMethodInfo fromType toType =
            match <@ Seq.map id Seq.empty<int> @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| fromType; toType |])
            | _ -> failwith "Compiler internal error"

        let seqCollectMethodInfo fromType toType =
            match <@ Seq.collect (fun (i: int) -> Seq.empty<string>) Seq.empty<int> @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| fromType; typedefof<seq<_>>.MakeGenericType([| toType |]); toType |])
            | _ -> failwith "Compiler internal error"

        let seqFilterMethodInfo genericType = 
            match <@ Seq.filter (fun _ -> true) Seq.empty<int> @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| genericType |])
            | _ -> failwith "Compiler internal error"

        let setToSeqMethodinfo genericType =
            match <@ Set.toSeq Set.empty<int> @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| genericType |])
            | _ -> failwith "Compiler internal error"

        let tupleSndMethodInfo leftType rightType =
            match <@ snd (1, 2) @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| leftType; rightType |])
            | _ -> failwith "Compiler internal error"

        let mapFindMethodInfo keyType valueType =
            typedefof<Map<_, _>>.MakeGenericType([| keyType; valueType |]).GetMethod("TryFind")

        let mapToSeqMethodInfo keyType valueType =
            match <@ Map.toSeq Map.empty<int, int> @> with
            | Call(_, methodInfo, _) -> methodInfo.GetGenericMethodDefinition().MakeGenericMethod([| keyType; valueType |])
            | _ -> failwith "Compiler internal error"

        let tableDataMapPropertyInfo recordType =
            typedefof<Table<_>>.MakeGenericType([| recordType |]).GetProperty("DataMap")

        let tableExprDataMapPropertyInfo (tableExpr: Expr) =
            tableExpr.Type.GetProperty("DataMap")

        let tableExprIndexesPropertyInfo (tableExpr: Expr) =
            tableExpr.Type.GetProperty("Indexes")

        let indexExprIndexMapPropertyInfo (indexExpr: Expr) =
            indexExpr.Type.GetProperty("IndexMap")

        let mapExprTryFindMethodInfo (mapExpr: Expr) =
            mapExpr.Type.GetMethod("TryFind")

        let optionExprNoneUnionCaseInfo (optionExpr: Expr) =
            FSharpType.GetUnionCases(optionExpr.Type) |> Array.find (fun uc -> uc.Name = "None")

        let optionExprSomeUnionCaseInfo (optionExpr: Expr) =
            FSharpType.GetUnionCases(optionExpr.Type) |> Array.find (fun uc -> uc.Name = "Some")
        
        let optionExprValuePropertyInfo (optionExpr: Expr) =
            optionExpr.Type.GetProperty("Value")

        let someUnionCaseInfo (t: Type) =
            FSharpType.GetUnionCases(typedefof<_ option>.MakeGenericType([| t |])) |> Array.find (fun uc -> uc.Name = "Some")

        let noneUnionCaseInfo (t: Type) =
            FSharpType.GetUnionCases(typedefof<_ option>.MakeGenericType([| t |])) |> Array.find (fun uc -> uc.Name = "None")

        let compileCollectByIndex attribute indexedRelationId idxName queryType =
            let env, bindingsBuilder = buildLambdaBodyBindings queryType

            let recordType = queryType.GetRecordType()
            let matchValueVar = Var("matchValue", typeof<Set<IComparable> option>, false)
            let matchValueExpr = Expr.Var matchValueVar
                
            let keysValueVar = Var("keys", typeof<Set<IComparable>>)
            let keysValueExpr = Expr.Var keysValueVar

            let keyValueVar = Var("key", typeof<IComparable>)
            let keyValueExpr = Expr.Var keyValueVar

            let argumentVar = Var("records", recordType, false)
            let argumentExpr = Expr.Var argumentVar

            let indexedRelationExpr = env.[indexedRelationId]

            let indexSelectPropertyExpr = Expr.PropertyGet(indexedRelationExpr, tableExprIndexesPropertyInfo indexedRelationExpr, [Expr.Value idxName])
            let indexMapSelectPropertyExpr = Expr.PropertyGet(
                                                indexSelectPropertyExpr,
                                                indexExprIndexMapPropertyInfo indexSelectPropertyExpr)

            let attributeVar = Var("attr", attribute.Property.DeclaringType)
            let attributeExpr = Expr.Var attributeVar

            let body = 
                Expr.Let(
                    attributeVar,
                    Expr.PropertyGet(indexedRelationExpr, attribute.Property),
                    Expr.Let(
                        matchValueVar,
                        Expr.Call(
                            indexMapSelectPropertyExpr,
                            mapExprTryFindMethodInfo indexMapSelectPropertyExpr,
                            [attributeExpr]),
                        Expr.IfThenElse(
                            Expr.UnionCaseTest(
                                matchValueExpr,
                                optionExprNoneUnionCaseInfo matchValueExpr),
                            getEmptySeqExpr recordType,
                            Expr.Let(
                                keysValueVar,
                                Expr.PropertyGet(
                                    matchValueExpr,
                                    optionExprValuePropertyInfo matchValueExpr),
                                Expr.Call(
                                    seqMapMethodInfo typeof<IComparable> recordType,
                                    [
                                        Expr.Lambda(
                                            keyValueVar,
                                            Expr.PropertyGet(
                                                indexedRelationExpr,
                                                tableExprDataMapPropertyInfo indexedRelationExpr,
                                                [ keyValueExpr ])
                                        )

                                        Expr.Coerce(keysValueExpr, typeof<seq<IComparable>>)
                                    ]
                                )
                            )
                        )
                    )
                )
                
            Expr.Lambda(argumentVar, bindingsBuilder argumentExpr body)

        let rec compileAccessPath ap = 
            match ap with
            | RelationScan(Atom Top, table) ->
                let recordType = table.RecordType
                let tupleType = typedefof<System.Tuple<_, _>>.MakeGenericType([| typeof<IComparable>; recordType |])
                let tupleVar = Var("tuple", tupleType, false)
                let tupleArg = Expr.Var tupleVar
                
                Expr.Call(seqMapMethodInfo tupleType recordType,
                          [ Expr.Lambda(tupleVar,
                                        Expr.Call(tupleSndMethodInfo typeof<IComparable> recordType,
                                                  [tupleArg]
                                                 )
                                       )
                            Expr.Call(mapToSeqMethodInfo typeof<IComparable> recordType,
                                      [ Expr.PropertyGet(table.Expr, tableExprDataMapPropertyInfo table.Expr, []) ])
                          ])

                //<@@ (%%table).DataMap |> Map.toSeq |> Seq.map snd @@> //Expr<seq<'T>> & Table<'T>
            | RelationScan(Atom Bottom, table) ->
                getEmptySeqExpr table.RecordType //Seq.empty<'T> & Table<'T>
            | RelationScan(sigma, table) ->
                let recordType = table.RecordType
                let tupleType = typedefof<System.Tuple<_, _>>.MakeGenericType([| typeof<IComparable>; recordType |])
                let tupleVar = Var("tuple", tupleType, false)
                let tupleArg = Expr.Var tupleVar

                let filter = compileSigma sigma table.QueryType
                
                Expr.Call(seqFilterMethodInfo recordType,
                          [ filter
                            Expr.Call(seqMapMethodInfo tupleType recordType,
                                      [ Expr.Lambda(tupleVar,
                                                    Expr.Call(tupleSndMethodInfo typeof<IComparable> recordType,
                                                              [tupleArg]
                                                             )
                                                   )
                                        Expr.Call(mapToSeqMethodInfo typeof<IComparable> recordType,
                                                  [ Expr.PropertyGet(table.Expr, tableExprDataMapPropertyInfo table.Expr, []) ])
                                      ])
                          ])

                //<@@ (%%table).DataMap |> Map.toSeq |> Seq.map snd |> Seq.filter (%%filter) @@> //Expr<seq<'T>> & Table<'T>
            | IndexEq(idxName, value, table) ->
                let recordType = table.RecordType
                let matchValueVar = Var("matchValue", typeof<Set<IComparable> option>, false)
                let matchValueExpr = Expr.Var matchValueVar
                
                let keysValueVar = Var("keys", typeof<Set<IComparable>>)
                let keysValueExpr = Expr.Var keysValueVar
                let keyValueVar = Var("key", typeof<IComparable>)
                let keyValueExpr = Expr.Var keyValueVar

                let indexSelectPropertyExpr = Expr.PropertyGet(table.Expr, tableExprIndexesPropertyInfo table.Expr, [Expr.Value idxName])
                let indexMapSelectPropertyExpr = Expr.PropertyGet(
                                                    indexSelectPropertyExpr,
                                                    indexExprIndexMapPropertyInfo indexSelectPropertyExpr)

                Expr.Let(
                    matchValueVar,
                    Expr.Call(
                        indexMapSelectPropertyExpr,
                        mapExprTryFindMethodInfo indexMapSelectPropertyExpr,
                        [value]),
                    Expr.IfThenElse(
                        Expr.UnionCaseTest(
                            matchValueExpr,
                            optionExprNoneUnionCaseInfo matchValueExpr),
                        getEmptySeqExpr recordType,
                        Expr.Let(
                            keysValueVar,
                            Expr.PropertyGet(
                                matchValueExpr,
                                optionExprValuePropertyInfo matchValueExpr),
                            Expr.Call(
                                seqMapMethodInfo typeof<IComparable> recordType,
                                [
                                    Expr.Lambda(
                                        keyValueVar,
                                        Expr.PropertyGet(
                                            table.Expr,
                                            tableExprDataMapPropertyInfo table.Expr,
                                            [ keyValueExpr ])
                                    )

                                    Expr.Coerce(keysValueExpr, typeof<seq<IComparable>>)
                                ]
                            )
                        )
                    )
                )
//                <@@ 
//                    match (%%table).Indexes.[idxName].IndexMap.TryFind value with
//                    | Some ks -> Seq.map (fun k -> (%%table).DataMap.[k]) ks
//                    | None -> Seq.empty
//                @@>  //Expr<seq<'T>> & Table<'T>
            | SequenceScan(Atom Top, seqPath) ->
                compileAccessPath seqPath //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
            | SequenceScan(Atom Bottom, seqPath) ->
                getEmptySeqExpr (seqPath.GetQueryType().GetRecordType()) //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
            | SequenceScan(sigma, seqPath) ->
                let sequenceExpr = compileAccessPath seqPath

                let queryType = seqPath.GetQueryType()
                let recordType = queryType.GetRecordType()

                let filter = compileSigma sigma queryType

                Expr.Call(
                    seqFilterMethodInfo recordType,
                    [ filter; sequenceExpr])

                //<@@ %%sequenceExpr |> Seq.filter %%filter @@> //Expr<Seq<'T>> & [[seqPath]] = Expr<Seq<'T>>
            | IndexedEqJoin(attribute, indexedRelationName, indexedRelationType, idxName, leftPath, rightPath) ->
                let leftSequenceExpr = compileAccessPath leftPath
                let rightSequenceExpr = compileAccessPath rightPath

                let leftQueryType = leftPath.GetQueryType()
                let leftRecordType = leftQueryType.GetRecordType()
                let rightQueryType = rightPath.GetQueryType()
                let rightRecordType = rightQueryType.GetRecordType()
                let outputQueryType = JoinType(leftQueryType, rightQueryType)
                let outputRecordType = outputQueryType.GetRecordType()

                let indexedRelationId = Attribute.MakeRelationId(indexedRelationName, indexedRelationType)

                invalidOp "FOO"

                //<@ %%leftSequenceExpr

            | PathJoin(sigma, Inner, leftPath, rightPath) ->
                let leftSequenceExpr = compileAccessPath leftPath
                let rightSequenceExpr = compileAccessPath rightPath
                
                let leftQueryType = leftPath.GetQueryType()
                let leftRecordType = leftQueryType.GetRecordType()
                let rightQueryType = rightPath.GetQueryType()
                let rightRecordType = rightQueryType.GetRecordType()
                let outputQueryType = JoinType(leftQueryType, rightQueryType)
                let outputRecordType = outputQueryType.GetRecordType()
                
                let filter = compileSigma sigma outputQueryType

                let collectExpr = 
                    let leftVar = Var("left", leftRecordType, false)
                    let rightVar = Var("right", rightRecordType, false)
                    let leftVarExpr = Expr.Var leftVar
                    let rightVarExpr = Expr.Var rightVar
 
                    Expr.Lambda(leftVar, 
                                Expr.Call(seqFilterMethodInfo outputRecordType, 
                                          [ filter 
                                            Expr.Call(seqMapMethodInfo rightRecordType outputRecordType, 
                                                      [ Expr.Lambda(rightVar, 
                                                                    Expr.NewTuple [leftVarExpr; rightVarExpr]
                                                                   ) 
                                                        rightSequenceExpr
                                                      ]
                                                     )
                                          ]
                                         )
                               )

                Expr.Call(seqCollectMethodInfo leftRecordType outputRecordType, [ collectExpr; leftSequenceExpr])

                //<@@ %%leftSequenceExpr |> Seq.collect %%collectExpr @@> //Expr<Seq<'T * 'U> & [[leftPath]] = Expr<Seq<'T> & [[rightPath]] = Expr<Seq<'U>>

            | PathJoin(sigma, LeftOuter, leftPath, rightPath) ->
                let leftSequenceExpr = compileAccessPath leftPath
                let rightSequenceExpr = compileAccessPath rightPath
                
                let leftQueryType = leftPath.GetQueryType()
                let leftRecordType = leftQueryType.GetRecordType()
                let rightQueryType = rightPath.GetQueryType()
                let rightRecordType = rightQueryType.GetRecordType()
                let filterQueryType = JoinType(leftQueryType, rightQueryType)
                let filterRecordType = filterQueryType.GetRecordType()
                let outputQueryType = JoinType(leftQueryType, rightQueryType |> Nullable)
                let outputRecordType = outputQueryType.GetRecordType()
                
                let filter = compileSigma sigma filterQueryType

                let mapExpr =
                    let leftVar = Var("left", leftRecordType, false)
                    let leftVarExpr = Expr.Var leftVar

                    Expr.Lambda(leftVar,
                                Expr.NewTuple([leftVarExpr; Expr.NewUnionCase(noneUnionCaseInfo rightRecordType, [])])
                               )

                let collectExpr = 
                    let leftVar = Var("left", leftRecordType, false)
                    let rightVar = Var("right", rightRecordType, false)
                    let leftVarExpr = Expr.Var leftVar
                    let rightVarExpr = Expr.Var rightVar
 
                    Expr.Lambda(leftVar, 
                                Expr.Call(seqMapMethodInfo rightRecordType outputRecordType,
                                          [ Expr.Lambda(rightVar,
                                                        Expr.NewTuple [ leftVarExpr
                                                                        Expr.IfThenElse(Expr.Application(filter,
                                                                                                         Expr.NewTuple [leftVarExpr; rightVarExpr]
                                                                                                        ),
                                                                                        Expr.NewUnionCase(someUnionCaseInfo rightRecordType, [ rightVarExpr ]),
                                                                                        Expr.NewUnionCase(noneUnionCaseInfo rightRecordType, [])
                                                                                       )
                                                                      ]
                                                       )
                                            rightSequenceExpr
                                          ]
                                         )
                               )

                Expr.IfThenElse(Expr.Call(seqIsEmptyMethodInfo rightRecordType, [rightSequenceExpr]),
                                Expr.Call(seqMapMethodInfo leftRecordType outputRecordType, [mapExpr; leftSequenceExpr]),
                                Expr.Call(seqCollectMethodInfo leftRecordType outputRecordType, [ collectExpr; leftSequenceExpr])
                               )

                //<@@ if %rightSequenceExpr |> Seq.isEmpty then  
                //      %%leftSequenceExpr |> Seq.map (fun left -> left, None)
                //    else
                //      %%leftSequenceExpr |> Seq.collect (fun left -> %rightSeqExpr |> Seq.map (fun right -> left, if %filter left right then Some right else None)  @@> //Expr<Seq<'T * ('U option)>

            | PathJoin(sigma, RightOuter, leftPath, rightPath) ->
                let leftSequenceExpr = compileAccessPath leftPath
                let rightSequenceExpr = compileAccessPath rightPath
                
                let leftQueryType = leftPath.GetQueryType()
                let leftRecordType = leftQueryType.GetRecordType()
                let rightQueryType = rightPath.GetQueryType()
                let rightRecordType = rightQueryType.GetRecordType()
                let filterQueryType = JoinType(leftQueryType, rightQueryType)
                let filterRecordType = filterQueryType.GetRecordType()
                let outputQueryType = JoinType(leftQueryType |> Nullable, rightQueryType)
                let outputRecordType = outputQueryType.GetRecordType()
                
                let filter = compileSigma sigma filterQueryType

                let mapExpr =
                    let rightVar = Var("right", rightRecordType, false)
                    let rightVarExpr = Expr.Var rightVar

                    Expr.Lambda(rightVar,
                                Expr.NewTuple([ Expr.NewUnionCase(noneUnionCaseInfo leftRecordType, [])
                                                rightVarExpr
                                             ])
                               )

                let collectExpr = 
                    let leftVar = Var("left", leftRecordType, false)
                    let rightVar = Var("right", rightRecordType, false)
                    let leftVarExpr = Expr.Var leftVar
                    let rightVarExpr = Expr.Var rightVar
 
                    Expr.Lambda(leftVar, 
                                Expr.Call(seqMapMethodInfo rightRecordType outputRecordType,
                                          [ Expr.Lambda(rightVar,
                                                        Expr.NewTuple [ 
                                                                        Expr.IfThenElse(Expr.Application(filter,
                                                                                                         Expr.NewTuple [leftVarExpr; rightVarExpr]
                                                                                                        ),
                                                                                        Expr.NewUnionCase(someUnionCaseInfo leftRecordType, [ leftVarExpr ]),
                                                                                        Expr.NewUnionCase(noneUnionCaseInfo leftRecordType, [])
                                                                                       )
                                                                        rightVarExpr
                                                                      ]
                                                       )
                                            rightSequenceExpr
                                          ]
                                         )
                               )

                Expr.IfThenElse(Expr.Call(seqIsEmptyMethodInfo leftRecordType, [leftSequenceExpr]),
                                Expr.Call(seqMapMethodInfo rightRecordType outputRecordType, [mapExpr; rightSequenceExpr]),
                                Expr.Call(seqCollectMethodInfo leftRecordType outputRecordType, [ collectExpr; leftSequenceExpr])
                               )

                //<@@ if %%leftSequenceExpr |> Seq.isEmpty then
                //      %%rightSequenceExpr |> Seq.map (fun right -> None, right)
                //    else %%leftSequenceExpr |> Seq.collect (fun left -> %rightSeqExpr |> Seq.map (fun right ->  if %filter left right then Some left else None, right)  @@> //Expr<Seq<('T option) * 'U>

        let optimizedCompile ap =
            let optimizedAccessPath = accessPathOptimize ap
            let compiledQuery = compileAccessPath optimizedAccessPath
            
            let resultType = optimizedAccessPath.GetQueryType()
            let resultRecordType = resultType.GetRecordType()
            let finalType = ap.GetQueryType()
            let finalRecordType = finalType.GetRecordType()
            
            Expr.Call(seqMapMethodInfo resultRecordType finalRecordType, 
                        [compileFinalMap resultType finalType; compiledQuery])

        let accessPath = translate relExpr 
        //optimizedCompile accessPath
        compileAccessPath accessPath



    let private relationNameFromExpr (expr: Expr) : string = 
        let value = Expr.PropertyGet(expr, expr.Type.GetProperty("Name")) |> Swensen.Unquote.Operators.evalRaw<string>
        value 

    let private decompileExprToSigma (queryShape: QueryShape) (expr: Expr<'T -> bool>) =
        let generalExpr expr varMap =
            GeneralExpr(
                expr, 
                expr.GetFreeVars() |> Seq.choose (fun var -> match varMap |> Map.tryFind var with 
                                                             | Some(Choice1Of2 tableExpr) -> Some(var, RelationVar(Attribute.MakeRelationId(relationNameFromExpr tableExpr, var.Type))) 
                                                             | Some(Choice2Of2 atom) -> Some(var, atom)
                                                             | _ -> None)
                                   |> Seq.toList)
                                                 
        let rec translate expr (varMap: Map<Var, Choice<Expr, SigmaAtom>>) =
            match expr with
            | Value((:? bool as boolean), _) when boolean = true -> Atom Top
            | Value((:? bool as boolean), _) when boolean = false -> Atom Bottom
            | Value _ as expr -> Atom(Literal expr)
//            | PropertyGet(Some(ShapeVar var), propertyInfo, []) as expr ->
//                match varMap.TryFind var with
//                | Some(Choice1Of2 tableExpr) ->
////                    Atom(Attr { RelationName = relationNameFromExpr tableExpr; Property = propertyInfo })
//                    Atom(AttrDeref(Direct({ RelationName = relationNameFromExpr tableExpr; Property = propertyInfo }, [])))
//                | Some(Choice2Of2(AttrDeref indirect)) ->
//                    Atom(AttrDeref(Indirect(indirect, expr)))
////                | Some(Choice2Of2((Attr attribute) | AttrDeref(attribute, _))) ->
////                    Atom(AttrDeref(attribute, expr))
//                | _ -> generalExpr expr varMap
            | PropertyGet(Some(ShapeVar var), propertyInfo, args) as expr ->
                match varMap.TryFind var with
                | Some(Choice1Of2 tableExpr) ->
                    let argsSigmaExprs = args |> List.map (fun expr -> generalExpr expr varMap)
                    Atom(AttrDeref(Direct({ RelationName = relationNameFromExpr tableExpr; Property = propertyInfo }, argsSigmaExprs)))
                | Some(Choice2Of2(AttrDeref indirect)) ->
                    Atom(AttrDeref(Indirect(indirect, expr)))
                | _ -> generalExpr expr varMap
            | PropertyGet(Some receiverExpr, pinfo, []) as expr -> 
                match translate receiverExpr varMap with
                | Atom(AttrDeref indirect) ->
                    Atom(AttrDeref(Indirect(indirect, Expr.PropertyGet(Expr.Var(new Var(Guid.NewGuid().ToString(), receiverExpr.Type, false)), pinfo, []))))
                | _ -> generalExpr expr varMap
//                match translate receiverExpr varMap with
//                | Atom(Attr attribute)
//                | Atom(AttrDeref(attribute, _)) -> Atom(AttrDeref(attribute, expr))
//                | _ -> generalExpr expr varMap
            | Let(var, aliasExpr, body) as expr ->
                match translate aliasExpr varMap with
                | Atom atom -> translate body (varMap |> Map.add var (Choice2Of2 atom))
                | _ -> generalExpr expr varMap
            //| IfThenElse(guardExpr, thenExpr, elseExpr) ->
//                let guard = translate guardExpr varMap
//                BinaryExpr(And,
//                    BinaryExpr(Or,
//                        UnaryExpr(Not, guard),
//                        translate thenExpr varMap),
//                    BinaryExpr(Or,
//                        guard,
//                        translate elseExpr varMap))
            | SpecificCall <@ (=) @> (_, _, [left; right]) -> BinaryExpr(Eq, translate left varMap, translate right varMap)
            | SpecificCall <@ (<>) @> (_, _, [left; right]) -> UnaryExpr(Not, BinaryExpr(Eq, translate left varMap, translate right varMap))
            | SpecificCall <@ (>) @> (_, _, [left; right]) -> BinaryExpr(Gt, translate left varMap, translate right varMap)
            | SpecificCall <@ (>=) @> (_, _, [left; right]) -> 
                let left', right' = translate left varMap, translate right varMap
                BinaryExpr(Or, BinaryExpr(Gt, left', right'), BinaryExpr(Eq, left', right'))
            | SpecificCall <@ (<) @> (_, _, [left; right]) -> BinaryExpr(Lt, translate left varMap, translate right varMap)
            | SpecificCall <@ (<=) @> (_, _, [left; right]) ->
                let left', right' = translate left varMap, translate right varMap
                BinaryExpr(Or, BinaryExpr(Lt, left', right'), BinaryExpr(Eq, left', right'))
            | SpecificCall <@ not @> (_, _, [arg]) -> UnaryExpr(Not, translate arg varMap)
            | _ as expr -> generalExpr expr varMap


        let rec queryShapeToTableList =
            function TableValue tableValueExpr -> [tableValueExpr]
                     | JoinValue(leftShape, rightShape) -> (queryShapeToTableList leftShape)@(queryShapeToTableList rightShape)

        let rec varToTableBindings letShape (varShapeMap: Map<Var, QueryShape>) =
            let rec (|TupleQueryShapeFollow|_|) letShape =
                match letShape with
                | TupleGet(ShapeVar var, 0) -> 
                    match varShapeMap.TryFind var with
                    | Some(JoinValue(queryShape, _)) -> Some queryShape
                    | _ -> None
                | TupleGet(ShapeVar var, 1) -> 
                    match varShapeMap.TryFind var with
                    | Some(JoinValue(_, queryShape)) -> Some queryShape
                    | _ -> None
                | TupleGet(TupleQueryShapeFollow queryShape, 0) ->
                    match queryShape with
                    | JoinValue(queryShape, _) -> Some queryShape
                    | _ -> None
                | TupleGet(TupleQueryShapeFollow queryShape, 1) ->
                    match queryShape with
                    | JoinValue(_, queryShape) -> Some queryShape
                    | _ -> None
                | _ -> None
                

            match letShape with
            | Let(var, TupleQueryShapeFollow(TableValue tableExpr), (Let _ as rest)) -> (var, tableExpr)::(varToTableBindings rest varShapeMap)
            | Let(var, TupleQueryShapeFollow(TableValue tableExpr), otherNotLet) -> [var, tableExpr]
            | Let(var, TupleQueryShapeFollow joinValue, (Let _ as rest)) -> varToTableBindings rest (varShapeMap |> Map.add var joinValue)
            | _ -> []

        let stagedTranslate var body =
            let varMap = 
                varToTableBindings body (Map.empty |> Map.add var queryShape) 
                |> List.map (fun (var, tableExpr) -> var, Choice1Of2 tableExpr) 
                |> Map.ofList
            
            let rec skipTranslate =
                function Let(_, TupleGet _, (Let _ as rest)) -> skipTranslate rest
                         | Let(_, TupleGet _, rest) | (Let _ as rest) -> translate rest
                         | _ -> failwith "Compiler error. Unable to translate join/where clause expression."

            skipTranslate body varMap

        match expr with
        | Lambda(var, body) when FSharpType.IsTuple var.Type -> stagedTranslate var body
        | Lambda(var, body) -> translate body (Map.empty |> Map.add var (Choice1Of2(queryShapeToTableList queryShape |> List.head)))
        | _ -> failwith "Compiler Error: Boolean function expected"


    type SequenceCompiler<'T> private() =
        static member Compile(queryExpression: RelationalExpr): seq<'T> =
            compile queryExpression |> Swensen.Unquote.Operators.evalRaw<seq<'T>>

    let private tableToRelationalExpr (table: Table<'T>) =
        Relation { 
            Name = table.Name 
            Expr = <@ table @>
            QueryType = RecordType(table.Name, typeof<'T>) 
            Cardinality = table.DataMap.Count
            IndexedAttributes = Map.empty
        }

    let private rename (newName: string) (table: Table<'T>): Table<'T> =
        { table with
            Name = newName
            Indexes = table.Indexes |> Map.map (fun _ idx -> { idx with Attribute = { idx.Attribute with RelationName = newName } })
        }

    let from (table: Table<'T>): Query<'T> = 
        let newName = Guid.NewGuid().ToString()
        { 
            RelationalQuery = table |> rename newName |> tableToRelationalExpr
            Compiler = SequenceCompiler.Compile
        }

    let crossJoin (table: Table<'U>) (query: Query<'T>): Query<'T * 'U> = 
        let newName = Guid.NewGuid().ToString()
        {
            RelationalQuery = Product(query.RelationalQuery, table |> rename newName |> tableToRelationalExpr)
            Compiler = SequenceCompiler.Compile
        }

    let innerJoin (table: Table<'U>) (joinCondition: Expr<('T * 'U) -> bool>) (query: Query<'T>): Query<'T * 'U> = 
        let newName = Guid.NewGuid().ToString()
        let table' = table |> rename newName
        let queryShape = JoinValue(query.RelationalQuery.GetQueryShape(), TableValue(<@ table' @>))
        let sigma = decompileExprToSigma queryShape joinCondition
        {
            RelationalQuery = InnerJoin(sigma, query.RelationalQuery, tableToRelationalExpr table')
            Compiler = SequenceCompiler.Compile
        }

    let leftOuterJoin (table: Table<'U>) (joinCondition: Expr<('T * 'U) -> bool>) (query: Query<'T>): Query<'T * ('U option)> =
        let newName = Guid.NewGuid().ToString()
        let table' = table |> rename newName
        let queryShape = JoinValue(query.RelationalQuery.GetQueryShape(), TableValue(<@ table' @>))
        let sigma = decompileExprToSigma queryShape joinCondition
        {
            RelationalQuery = LeftOuterJoin(sigma, query.RelationalQuery, tableToRelationalExpr table')
            Compiler = SequenceCompiler.Compile
        }

    let rightOuterJoin (table: Table<'U>) (joinCondition: Expr<('T * 'U) -> bool>) (query: Query<'T>): Query<('T option) * 'U> =
        let newName = Guid.NewGuid().ToString()
        let table' = table |> rename newName
        let queryShape = JoinValue(query.RelationalQuery.GetQueryShape(), TableValue(<@ table' @>))
        let sigma = decompileExprToSigma queryShape joinCondition
        {
            RelationalQuery = RightOuterJoin(sigma, query.RelationalQuery, tableToRelationalExpr table')
            Compiler = SequenceCompiler.Compile
        }

    let where (filter: Expr<'T -> bool>) (query: Query<'T>): Query<'T> = {
        RelationalQuery = Sigma(decompileExprToSigma (query.RelationalQuery.GetQueryShape()) filter, query.RelationalQuery)
        Compiler = SequenceCompiler.Compile
    }

    let toSeq (query: Query<'T>): seq<'T> = query.Compiler query.RelationalQuery
        

module Table = 
    let create (pkeyExpr: Expr<'T -> 'U> when 'U :> IComparable): Table<'T> =
        let pkeyF = Swensen.Unquote.Operators.eval pkeyExpr
        let indexes =
            match pkeyExpr with
            | Lambda(var, PropertyGet(Some(ShapeVar var'), propertyInfo, [])) when var = var' ->
                Map.empty |> Map.add (Guid.Empty.ToString()) {
                        IndexName = Guid.Empty.ToString()
                        Attribute = { RelationName = String.Empty; Property = propertyInfo }
                        IndexKey = fun r -> pkeyF r :> IComparable
                        IndexMap = Map.empty
                    }
            | _ -> Map.empty
        {
            Name = String.Empty
            PrimaryKey = fun r -> pkeyF r :> IComparable
            DataMap = Map.empty
            Indexes = indexes
        }

    let createIndex (idxName: string) (attributeExpr: Expr<'T -> 'U> when 'U :> IComparable) (table: Table<'T>): Table<'T> =
        let indexedAttribute =
            function Lambda(var, PropertyGet(Some(ShapeVar var'), propertyInfo, [])) when var = var' -> { RelationName = table.Name; Property = propertyInfo }
                     | _ -> failwith "Invalid attribute selection expression."

        let indexF = attributeExpr |> Swensen.Unquote.Operators.eval
        let indexKeyF = fun record -> indexF record :> IComparable 

        let index = {
            IndexName = idxName
            Attribute = indexedAttribute attributeExpr
            IndexKey = indexKeyF
            IndexMap = table.DataMap |> Map.toSeq |> Seq.map (fun (k, record) -> indexKeyF record, k) 
                                                  |> Seq.groupBy fst
                                                  |> Seq.map (fun (indexKey, keys) -> indexKey, keys |> Seq.map snd |> Set.ofSeq)
                                                  |> Map.ofSeq
        }

        { table with Indexes = table.Indexes |> Map.add idxName index }

    let deleteIndex (idxName: string) (table: Table<'T>): Table<'T> =
        { table with Indexes = table.Indexes |> Map.remove idxName }

    let insert (record: 'T) (table: Table<'T>): Table<'T> = 
        let key = table.PrimaryKey record
        { table with
            DataMap = table.DataMap |> Map.add key record
            Indexes = table.Indexes |> Map.map (fun _ idx -> let indexKey = idx.IndexKey record in { idx with IndexMap = idx.IndexMap |> Map.add indexKey (match idx.IndexMap.TryFind indexKey with Some keys -> keys |> Set.add key | None -> Set.singleton key ) }) 
        }

    let insertValues (table: Table<'T>) (records: #seq<'T>): Table<'T> = 
        records |> Seq.fold (fun table record -> insert record table) table

    let ofSeq (pkeyExpr: Expr<'T -> 'U> when 'U :> IComparable) (records: #seq<'T>): Table<'T> =
        insertValues (create pkeyExpr) records

    let remove (key: 'U when 'U :> IComparable) (table: Table<'T>): Table<'T> =
        let key = key :> IComparable
        match table.DataMap.TryFind key with
        | Some record ->
            { table with
                DataMap = table.DataMap |> Map.remove key
                Indexes = table.Indexes |> Map.map (fun _ idx -> let indexKey = idx.IndexKey record in { idx with IndexMap = match idx.IndexMap.TryFind indexKey with Some keys -> (let keys' = keys |> Set.remove key in if keys |> Set.isEmpty then idx.IndexMap |> Map.remove indexKey else idx.IndexMap |> Map.add indexKey keys') | None -> idx.IndexMap })
            }
        | None -> table

    let delete (where: Expr<'T -> bool>) (table: Table<'T>): Table<'T> =
        Query.from table
        |> Query.where where
        |> Query.toSeq
        |> Seq.map table.PrimaryKey
        |> Seq.fold (fun table key -> table |> remove key) table

module Key = 
    [<Sealed>]
    type Composite internal (comparables: IComparable []) =
        member private c.Comparables = comparables

        member c.CompareTo(other: obj) =
            match other with
            | :? Composite as composite' when composite'.Comparables.Length = comparables.Length ->
                comparables |> Seq.compareWith (fun l r -> l.CompareTo(r)) composite'.Comparables
            | _ -> invalidOp "Uncomparable objects"

        override c.Equals(other: obj) = c.CompareTo(other) = 0
        override c.GetHashCode() = comparables |> Seq.sumBy (fun comparable -> comparable.GetHashCode())

        override c.ToString() = 
            sprintf "%A" (comparables |> Array.map (fun c -> c.ToString()))

        interface IComparable with
            member c.CompareTo(other: obj) = c.CompareTo(other)

    let composite (comparables: IComparable []) = Composite(comparables)
    

module Database =
    let private extractTable database tableExpr =
        database |> Swensen.Unquote.Operators.eval tableExpr

    let private updatedTableList (db: 'Db) (table: Table<'Record>) =
        [ for tableProperty in FSharpType.GetRecordFields(typeof<'Db>, BindingFlags.Instance|||BindingFlags.Public) ->  //typeof<'Db>.GetProperties(BindingFlags.Instance|||BindingFlags.Public) ->
            let tableType = tableProperty.PropertyType
            if tableType = typeof<Table<'Record>> then Expr.Value table
            else Expr.PropertyGet(Expr.Value db, tableProperty)
        ]

    let private updateDb (database: 'D) (table: Table<'T>) =
        let lst = updatedTableList database table
        Expr.NewRecord(typeof<'D>, lst)
        |> Expr.Cast 
        |> Swensen.Unquote.Operators.eval

    let private update database tableExpr tableUpdate =
        extractTable database tableExpr |> tableUpdate |> updateDb database

    let createIndex (tableExpr: Expr<'D -> Table<'T>>) 
                    (idxName: string) 
                    (attributeExpr: Expr<'T -> 'U> when 'U :> IComparable) 
                    (database: 'D): 'D =
        update database tableExpr (Table.createIndex idxName attributeExpr)

    let deleteIndex (tableExpr: Expr<'D -> Table<'T>>)
                    (idxName: string)
                    (database: 'D): 'D =
        update database tableExpr (Table.deleteIndex idxName)

    let insert (tableExpr: Expr<'D -> Table<'T>>) (record: 'T) (database: 'D): 'D =
        update database tableExpr (Table.insert record)

    let insertValues (tableExpr: Expr<'D -> Table<'T>>) (database: 'D) (records: #seq<'T>): 'D =
        update database tableExpr (fun tbl -> Table.insertValues tbl records) 

    let remove (tableExpr: Expr<'D -> Table<'T>>) (key: #IComparable) (database: 'D): 'D =
        update database tableExpr (Table.remove key)

    let delete (tableExpr: Expr<'D -> Table<'T>>) (where: Expr<'T -> bool>) (database: 'D): 'D =
        update database tableExpr (Table.delete where)
