package io.exsql.bytegraph.codegen

import java.io.StringReader
import java.security.{Permissions, ProtectionDomain, SecureClassLoader}
import ByteGraphCodeGenClassLoaderHelper.{RestrictedClassLoader, SynchronizedRestrictedClassLoader, UnsafeRestrictedClassLoader}
import org.codehaus.janino.util.ClassFile
import org.codehaus.janino.{ClassLoaderIClassLoader, Parser, Scanner, UnitCompiler}

import java.util

sealed trait ByteGraphCodeGenClassLoaderHelper {
  def getClassLoader: RestrictedClassLoader
}

private class ByteGraphCodeGenClassLoaderHelperThreadLocal extends ByteGraphCodeGenClassLoaderHelper {
  private val classLoaders: ThreadLocal[RestrictedClassLoader] = new ThreadLocal[RestrictedClassLoader]()
  override def getClassLoader: RestrictedClassLoader = {
    if (classLoaders.get() == null) {
      classLoaders.set(new UnsafeRestrictedClassLoader(Thread.currentThread().getContextClassLoader))
    }

    classLoaders.get()
  }
}

private class ByteGraphCodeGenClassLoaderHelperShared extends ByteGraphCodeGenClassLoaderHelper {
  private val classLoader: RestrictedClassLoader = new SynchronizedRestrictedClassLoader(
    new UnsafeRestrictedClassLoader(ClassLoader.getSystemClassLoader)
  )

  override def getClassLoader: RestrictedClassLoader = classLoader
}

object ByteGraphCodeGenClassLoaderHelper {

  val PackageName: String = "io.exsql.bytegraph.codegen.runtime"

  private val shared: ByteGraphCodeGenClassLoaderHelper = new ByteGraphCodeGenClassLoaderHelperShared

  private val threadLocal: ByteGraphCodeGenClassLoaderHelper = new ByteGraphCodeGenClassLoaderHelperThreadLocal

  sealed trait Mod {
    val name: String
  }

  case object Shared extends Mod {
    override val name: String = "shared"
  }

  case object ThreadLocal extends Mod {
    override val name: String = "thread-local"
  }

  def apply(mod: Mod): ByteGraphCodeGenClassLoaderHelper = {
    mod match {
      case Shared => shared
      case ThreadLocal => threadLocal
    }
  }

  def apply(modName: String): ByteGraphCodeGenClassLoaderHelper = {
    modName match {
      case Shared.name => shared
      case ThreadLocal.name => threadLocal
    }
  }

  sealed trait RestrictedClassLoader {
    def compile(className: String, classBody: String): Class[_]
    def tryLoadClass(name: String): Option[Class[_]]
  }

  final class UnsafeRestrictedClassLoader(private val parentClassLoader: ClassLoader) extends SecureClassLoader(parentClassLoader) with RestrictedClassLoader {

    override def compile(className: String, classBody: String): Class[_] = {
      val unitCompiler = new UnitCompiler(
        new Parser(new Scanner(null, new StringReader(classBody))).parseAbstractCompilationUnit(),
        new ClassLoaderIClassLoader(this)
      )


      val classFiles = new util.ArrayList[ClassFile]()
      unitCompiler.compileUnit(false, false, false, classFiles)

      val classBytes = classFiles.get(0).toByteArray
      defineClass(className, classBytes)
    }

    override def tryLoadClass(name: String): Option[Class[_]] = {
      try Some(this.loadClass(name))
      catch {
        case _: ClassNotFoundException => None
      }
    }

    private def defineClass(name: String, bytes: Array[Byte]): Class[_] = {
      defineClass(s"$PackageName.$name", bytes, 0, bytes.length, new ProtectionDomain(null, new Permissions, this, null))
    }

  }

  final class SynchronizedRestrictedClassLoader(private val unsafeRestrictedClassLoader: UnsafeRestrictedClassLoader) extends SecureClassLoader(unsafeRestrictedClassLoader) with RestrictedClassLoader {
    override def compile(className: String, classBody: String): Class[_] = unsafeRestrictedClassLoader.synchronized {
      unsafeRestrictedClassLoader.compile(className, classBody)
    }

    override def tryLoadClass(name: String): Option[Class[_]] = unsafeRestrictedClassLoader.tryLoadClass(name)
  }

}
