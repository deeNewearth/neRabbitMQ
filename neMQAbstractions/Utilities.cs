using System;
using System.Collections.Generic;
using System.Text;

namespace neMQConnector
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class ExportAsOptionalAttribute : Attribute
    {

    }
}
