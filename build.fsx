// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/build/FAKE/tools/"
#r "packages/build/FAKE/tools/FakeLib.dll"

open Fake 
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

open System
open System.IO

// --------------------------------------------------------------------------------------
// START TODO: Provide project-specific details below
// --------------------------------------------------------------------------------------

// Information about the project are used
//  - for version and project name in generated AssemblyInfo file
//  - by the generated NuGet package 
//  - to run tests and to publish documentation on GitHub gh-pages
//  - for documentation, you also need to edit info in "docs/tools/generate.fsx"

// The name of the project 
// (used by attributes in AssemblyInfo, name of a NuGet package and directory in 'src')
let project = "Thespian"

// Short summary of the project
// (used as description in AssemblyInfo and as a short summary for NuGet package)
let summary = "An F# Actor Framework"

// File system information 
let solutionFile  = "Thespian.sln"

// Pattern specifying assemblies to be tested using NUnit
// NOTE : No need to specify different directories.
let testProjects = [ "tests/Thespian.Tests" ]

let gitOwner = "mbraceproject"
// Git configuration (used for publishing documentation in gh-pages branch)
// The profile where the project is posted 
let gitHome = "https://github.com/" + gitOwner
// The name of the project on GitHub
let gitName = "Thespian"

let configuration = "Release"
let netcoreappTarget = "netcoreapp3.1"

let artifactsDir = __SOURCE_DIRECTORY__ @@ "artifacts"

// --------------------------------------------------------------------------------------
// END TODO: The rest of the file includes standard build steps 
// --------------------------------------------------------------------------------------

// Read additional information from the release notes document
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md")

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
  CreateFSharpAssemblyInfo "src/Thespian/AssemblyInfo.fs"
      [ Attribute.Title project
        Attribute.Product project
        Attribute.Description summary
        Attribute.Version release.AssemblyVersion
        Attribute.FileVersion release.AssemblyVersion ]
)

// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages

Target "Clean" (fun _ ->
    CleanDirs [artifactsDir]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->
    DotNetCli.Build (fun c ->
        { c with
            Project = solutionFile
            Configuration = configuration })
)

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner

Target "RunTests" (fun _ ->
    for proj in testProjects do
        DotNetCli.Test (fun p ->
            { p with
                Project = proj
#if MONO
                Framework = netcoreappTarget
#endif
                Configuration = configuration
                AdditionalArgs = ["--no-build"] })
)

//
//// --------------------------------------------------------------------------------------
//// Build a NuGet package

Target "NuGet" (fun _ ->    
    DotNetCli.Pack (fun p ->
        { p with
            OutputPath = artifactsDir
            Configuration = configuration
            Project = "src/Thespian"
            AdditionalArgs =
                [ sprintf "-p:PackageVersion=%s" release.NugetVersion
                  sprintf "-p:PackageReleaseNotes=\"%s\"" (String.concat Environment.NewLine release.Notes) ]
        })
)

Target "NuGetPush" (fun _ -> 
    Paket.Push (fun p -> 
        { p with 
            PublishUrl = "https://www.nuget.org"
            WorkingDir = artifactsDir }))

// --------------------------------------------------------------------------------------
// Release Scripts

Target "GenerateDocs" (fun _ ->
    let path = __SOURCE_DIRECTORY__ @@ "packages/build/FSharp.Compiler.Tools/tools/fsi.exe"
    let workingDir = "docs/tools"
    let args = "--define:RELEASE generate.fsx"
    let command, args = 
        if EnvironmentHelper.isMono then "mono", sprintf "'%s' %s" path args 
        else path, args

    if Shell.Exec(command, args, workingDir) <> 0 then
        failwith "failed to generate docs"
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    fullclean tempDocsDir
    CopyRecursive "docs/output" tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// Github Releases

#nowarn "85"
#load "paket-files/build/fsharp/FAKE/modules/Octokit/Octokit.fsx"
open Octokit

Target "ReleaseGitHub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.pushBranch "" remote (Information.getBranchName "")

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "OctokitToken" with
        | null -> 
            let user =
                match getBuildParam "github-user" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserInput "Username: "
            let pw =
                match getBuildParam "github-pw" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserPassword "Password: "

            createClient user pw
        | token -> createClientWithToken token

    // release on github
    client
    |> createDraft gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes
    |> releaseDraft
    |> Async.RunSynchronously
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Default" DoNothing
Target "Release" DoNothing
Target "Debug" DoNothing
Target "Bundle" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Default"
  ==> "NuGet"
  ==> "GenerateDocs"
  ==> "Bundle"

"Bundle"
  ==> "ReleaseDocs"
  ==> "NuGetPush"
  ==> "ReleaseGitHub"
  ==> "Release"

RunTargetOrDefault "Default"
